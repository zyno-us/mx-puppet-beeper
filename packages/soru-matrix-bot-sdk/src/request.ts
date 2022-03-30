import got from "got";

let requestFn = got;

/**
 * Sets the function to use for performing HTTP requests. Must be compatible with `got`.
 * @param fn The new request function.
 * @category Unit testing
 */
export function setRequestFn(fn) {
    requestFn = fn;
}

/**
 * Gets the `got`-compatible function for performing HTTP requests.
 * @returns The request function.
 * @category Unit testing
 */
export function getRequestFn() {
    return requestFn;
}
