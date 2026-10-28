import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { StorageEngine } from '../src/storage/engine.ts';
import {
  encodeGeohash52,
  decodeGeohash52,
  haversineDistanceMeters,
} from '../src/storage/geo.ts';

describe('Geospatial Indexing Engine', () => {
  it('encodes and decodes 52-bit geohash coordinates accurately', () => {
    const lon = -107.394;
    const lat = 24.809; // Culiacán coordinates
    const hash = encodeGeohash52(lon, lat);
    const decoded = decodeGeohash52(hash);

    assert.ok(Math.abs(decoded.longitude - lon) < 0.0001);
    assert.ok(Math.abs(decoded.latitude - lat) < 0.0001);
  });

  it('calculates great-circle Haversine distances accurately', () => {
    // Palermo to Catania (~166.4 km)
    const meters = haversineDistanceMeters(13.361389, 38.115556, 15.087269, 37.502669);
    assert.ok(meters > 166000 && meters < 167000);
  });

  it('supports GEOADD, GEOPOS, GEODIST, and GEOSEARCH through StorageEngine', () => {
    const engine = new StorageEngine();
    const added = engine.geoadd('Sicily', [
      { longitude: 13.361389, latitude: 38.115556, member: 'Palermo' },
      { longitude: 15.087269, latitude: 37.502669, member: 'Catania' },
    ]);
    assert.equal(added, 2);

    const positions = engine.geopos('Sicily', ['Palermo', 'NonExistent']);
    assert.equal(positions.length, 2);
    assert.ok(positions[0] !== null);
    assert.equal(positions[1], null);

    const distKm = engine.geodist('Sicily', 'Palermo', 'Catania', 'km');
    assert.ok(distKm !== null);
    assert.ok(distKm > 165 && distKm < 168);

    const nearby = engine.geosearch(
      'Sicily',
      { longitude: 15.0, latitude: 37.5 },
      100,
      'km'
    );
    assert.equal(nearby.length, 1);
    assert.equal(nearby[0]?.member, 'Catania');
  });
});
