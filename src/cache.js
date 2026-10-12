// LRU cache with TTL. Map keeps insertion order, so the first key is the least recently used.
export class LruCache {
  #map = new Map();
  #max;
  #ttlMs;
  #now;
  #hits = 0;
  #misses = 0;

  constructor({ max = 500, ttlMs = 60_000, now = Date.now } = {}) {
    this.#max = max;
    this.#ttlMs = ttlMs;
    this.#now = now;
  }

  get(key) {
    const entry = this.#map.get(key);
    if (!entry) {
      this.#misses++;
      return undefined;
    }
    if (entry.expires <= this.#now()) {
      this.#map.delete(key);
      this.#misses++;
      return undefined;
    }
    this.#map.delete(key);
    this.#map.set(key, entry);
    this.#hits++;
    return entry.value;
  }

  set(key, value) {
    this.#map.delete(key);
    this.#map.set(key, { value, expires: this.#now() + this.#ttlMs });
    if (this.#map.size > this.#max) this.#map.delete(this.#map.keys().next().value);
  }

  stats() {
    const lookups = this.#hits + this.#misses;
    return {
      size: this.#map.size,
      max: this.#max,
      hits: this.#hits,
      misses: this.#misses,
      hitRate: lookups ? this.#hits / lookups : 0,
    };
  }
}
