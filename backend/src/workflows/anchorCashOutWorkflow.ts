/**
 * Anchor Cash-Out Failure / Retry Handling Workflow (#1238)
 *
 * Provides a structured, observable retry path for anchor cash-out operations
 * that previously surfaced raw errors with no automated recovery.
 *
 * This workflow orchestrates:
 *   - SEP-10 authentication with the anchor.
 *   - Initiation of a SEP-31 cross-border payment (cash-out).
 *   - Status tracking via Sep31TrackingService.
 *   - Escalation when all retry attempts are exhausted.
 *
 * Workflow steps:
 *   1. validate-input      – schema validation.
 *   2. authenticate        – SEP-10 token exchange with the anchor.
 *   3. initiate-cashout    – POST the SEP-31 transaction.
 *   4. update-tracking     – Record the transaction in Sep31TrackingService.
 *   5. poll-status         – Poll until terminal status or max polls exhausted.
 */

import { Keypair } from '@stellar/stellar-sdk';
import { z } from 'zod';
import {
  requireOrganization,
  WorkflowError,
  WorkflowRun,
  NotReadyError,
  type WorkflowContext,
} from './runtime.js';
import { AnchorService } from '../services/anchorService.js';
import { Sep31TrackingService, type Sep31Transaction } from '../services/sep31TrackingService.js';
import { pool } from '../config/database.js';

// ── Input schema ──────────────────────────────────────────────────────────────

export const anchorCashOutInputSchema = z
  .object({
    /** Domain of the anchor (e.g. "anchor.example.com"). */
    anchorDomain: z.string().min(1, 'anchorDomain is required'),
    /** Stellar secret key of the employee initiating the cash-out. */
    clientSecret: z.string().min(1, 'clientSecret is required'),
    /** The Sep31Transaction record id already created by the caller, if any. */
    transactionId: z.string().uuid('transactionId must be a UUID').optional(),
    /** SEP-31 payment payload sent to the anchor. */
    paymentData: z.record(z.unknown()),
    /** Maximum number of status-poll attempts (default 5). */
    maxPollAttempts: z.number().int().min(1).max(20).optional(),
    /** Delay in ms between polls (default 2000). */
    pollIntervalMs: z.number().int().min(100).optional(),
  })
  .strict();

export type AnchorCashOutInput = z.infer<typeof anchorCashOutInputSchema>;

// ── Workflow ──────────────────────────────────────────────────────────────────

export class AnchorCashOutWorkflow {
  constructor(
    private readonly anchorService = AnchorService,
    private readonly trackingServiceFactory: (p: typeof pool) => Sep31TrackingService = (p) =>
      new Sep31TrackingService(p),
    private readonly dbPool = pool,
    private readonly delayMs = 100
  ) {}

  /**
   * Execute the anchor cash-out workflow with automatic retry.
   *
   * @param context - JWT payload of the authenticated caller.
   * @param input   - Anchor domain, credentials, and payment parameters.
   */
  async execute(context: WorkflowContext, input: AnchorCashOutInput) {
    const run = new WorkflowRun('anchor-cashout', context, this.delayMs);

    return run.execute(async () => {
      requireOrganization(context);
      const trackingService = this.trackingServiceFactory(this.dbPool);
      const maxPollAttempts = input.maxPollAttempts ?? 5;
      const pollIntervalMs = input.pollIntervalMs ?? 2000;

      // ── Step 1: Validate input ────────────────────────────────────────────
      const validated = await run.step(
        'validate-input',
        async () => {
          const result = anchorCashOutInputSchema.safeParse(input);
          if (!result.success) {
            throw new WorkflowError(
              'INVALID_INPUT',
              `Input validation failed: ${result.error.message}`
            );
          }
          return result.data;
        },
        /* retrySafe */ false
      );

      let clientKeypair: Keypair;
      try {
        clientKeypair = Keypair.fromSecret(validated.clientSecret);
      } catch {
        throw new WorkflowError('INVALID_INPUT', 'Invalid client secret key.');
      }

      // ── Step 2: Authenticate with the anchor (SEP-10) ─────────────────────
      const token = await run.step('authenticate', () =>
        this.anchorService.authenticate(validated.anchorDomain, clientKeypair)
      );

      // ── Step 3: Initiate the cash-out via SEP-31 ──────────────────────────
      const initiateResponse = await run.step('initiate-cashout', () =>
        this.anchorService.initiatePayment(
          validated.anchorDomain,
          token,
          validated.paymentData
        )
      );

      const anchorTransactionId: string =
        initiateResponse?.id ??
        initiateResponse?.transaction_id ??
        initiateResponse?.transaction?.id;

      if (!anchorTransactionId) {
        throw new WorkflowError(
          'ANCHOR_ERROR',
          'Anchor did not return a transaction identifier after initiation.'
        );
      }

      // ── Step 4: Update SEP-31 tracking record ────────────────────────────
      let trackedTx: Sep31Transaction | null = null;

      if (validated.transactionId) {
        trackedTx = await run.step('update-tracking', () =>
          trackingService.updateStatus(
            validated.transactionId!,
            'PROCESSING',
            undefined,
            undefined
          )
        );
      }

      // ── Step 5: Poll anchor for terminal status ───────────────────────────
      const finalStatus = await run.step('poll-status', async () => {
        for (let attempt = 1; attempt <= maxPollAttempts; attempt++) {
          const statusResponse = await this.anchorService
            .getTransaction(validated.anchorDomain, token, anchorTransactionId)
            .catch((err: Error) => {
              // Treat network errors as transient so the step-level retry kicks in.
              throw Object.assign(err, { response: { status: 503 } });
            });

          const txStatus: string =
            statusResponse?.transaction?.status ?? statusResponse?.status ?? 'pending';

          if (txStatus === 'completed' || txStatus === 'success') {
            if (trackedTx) {
              await trackingService.updateStatus(trackedTx.id, 'COMPLETED').catch(() => {});
            }
            return { anchorTransactionId, status: 'completed' as const };
          }

          if (
            txStatus === 'error' ||
            txStatus === 'expired' ||
            txStatus === 'refunded'
          ) {
            if (trackedTx) {
              await trackingService
                .updateStatus(trackedTx.id, 'FAILED', undefined, `Anchor status: ${txStatus}`)
                .catch(() => {});
            }
            throw new WorkflowError(
              'CASHOUT_FAILED',
              `Anchor reported terminal failure: ${txStatus}`
            );
          }

          if (attempt < maxPollAttempts) {
            // Not yet terminal — wait before next poll.
            await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
            // Signal to the step runner that we are still waiting.
            if (attempt === maxPollAttempts - 1) {
              throw new NotReadyError(`Anchor transaction ${anchorTransactionId} still pending`);
            }
          }
        }

        // Exhausted all polls without a terminal status — escalate.
        if (trackedTx) {
          await trackingService
            .updateStatus(
              trackedTx.id,
              'FAILED',
              undefined,
              'Max poll attempts exhausted — manual review required'
            )
            .catch(() => {});
        }
        throw new WorkflowError(
          'CASHOUT_TIMEOUT',
          `Anchor transaction ${anchorTransactionId} did not reach a terminal status after ${maxPollAttempts} polls.`
        );
      });

      return {
        anchorTransactionId,
        trackedTransactionId: trackedTx?.id ?? null,
        finalStatus,
      };
    });
  }
}
