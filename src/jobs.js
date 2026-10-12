export class ValidationError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ValidationError';
    this.code = 'BAD_REQUEST';
  }
}

const int = (name, value, min, max) => {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new ValidationError(`${name} must be an integer between ${min} and ${max}`);
  }
  return value;
};

const str = (name, value, max) => {
  if (typeof value !== 'string' || value.length === 0 || value.length > max) {
    throw new ValidationError(`${name} must be a string of 1 to ${max} characters`);
  }
  return value;
};

// each definition returns normalized params, so equal jobs produce equal cache keys
export const definitions = {
  fib: (p) => ({ n: int('n', p.n, 0, 45) }),
  primes: (p) => ({ limit: int('limit', p.limit, 2, 50_000_000) }),
  hash: (p) => ({ text: str('text', p.text, 10_000), rounds: int('rounds', p.rounds, 1, 5_000_000) }),
  wordfreq: (p) => ({ text: str('text', p.text, 200_000), top: int('top', p.top ?? 10, 1, 100) }),
  sleep: (p) => ({ ms: int('ms', p.ms, 1, 10_000) }),
};

export const jobTypes = Object.keys(definitions);

export function validate(type, params) {
  const definition = definitions[type];
  if (!definition) throw new ValidationError(`unknown job type "${type}", expected one of: ${jobTypes.join(', ')}`);
  if (params === null || typeof params !== 'object' || Array.isArray(params)) {
    throw new ValidationError('params must be an object');
  }
  return definition(params);
}

function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map((k) => [k, sortKeys(value[k])]));
  }
  return value;
}

export function cacheKey(type, params) {
  return `${type}:${JSON.stringify(sortKeys(params))}`;
}
