const h1 = require('farmhash');
const h2 = require('farmhash.wasm');
const h3 = require('xxhashjs');

const LOOPS = 100000;
const SEED = 0;

function process(name, alg, algKey) {
  const start_time = Date.now();
  const results = [];
  for (let i = 0; i < LOOPS; i++) {
    results.push(alg[algKey](i.toString(), SEED));
  }
  const elapsed = Date.now() - start_time;
  console.log(`${name} took ${elapsed}ms`);
  return {
    name,
    results
  };
}

function compareResults(result1, result2) {
  let errors = 0;
  for (let i = 0; i < LOOPS; i++) {
    if (result1.results[i] !== result2.results[i]) {
      errors++;
    }
  }

  if (errors > 0) {
    console.error(`${result1.name} results DO NOT MATCH ${result2.name}. ${errors} failures out of ${LOOPS}`);
  }
}

const farm32 = process('farmhash.hash32', h1, 'hash32');
const farm32Seed = process('farmhash.hash32WithSeed', h1, 'hash32WithSeed');
const farm64 = process('farmhash.hash64', h1, 'hash64');
const farm64Seed = process('farmhash.hash64WithSeed', h1, 'hash64WithSeed');
const farmWasm32 = process('farmhash.wasm.hash32', h2, 'hash32');
const farmWasm32WithSeed = process('farmhash.wasm.hash32WithSeed', h2, 'hash32WithSeed');
const farmWasm64 = process('farmhash.wasm.hash64', h2, 'hash64');
const farmWasm64WithSeed = process('farmhash.wasm.hash64WithSeed', h2, 'hash64WithSeed');
const xxhashjs32 = process('xxhashjs.h32', h3, 'h32');
const xxhashjs64 = process('xxhashjs.h64', h3, 'h64');

compareResults(farm32, farmWasm32);
compareResults(farm32Seed, farmWasm32WithSeed);
compareResults(farm64, farmWasm64);
compareResults(farm64Seed, farmWasm64WithSeed);
