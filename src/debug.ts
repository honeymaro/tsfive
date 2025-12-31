/**
 * Debug logging utilities for conditional output
 */

/**
 * Log a debug message if debug mode is enabled
 */
export function debugLog(debug: boolean | undefined, ...args: unknown[]): void {
  if (debug) {
    console.log(...args);
  }
}

/**
 * Log a debug warning if debug mode is enabled
 */
export function debugWarn(debug: boolean | undefined, ...args: unknown[]): void {
  if (debug) {
    console.warn(...args);
  }
}
