/**
 * Geospatial Indexing Codec, Morton 52-bit Geohash Spatial Windows, and Haversine Geodesics.
 *
 * Implements Redis-compatible EPSG:3857 52-bit interleaved integer Geohash scores
 * (26 bits longitude, 26 bits latitude), WGS-84 11-character Base32 Geohash strings,
 * Haversine great-circle distances, unit conversions, and 9-neighbor Morton prefix
 * score ranges for O(log N + K) spatial queries over Order-Statistic SkipLists.
 */

export const GEO_LON_MIN = -180.0;
export const GEO_LON_MAX = 180.0;
export const GEO_LAT_MIN = -85.05112878;
export const GEO_LAT_MAX = 85.05112878;

export const WGS84_LAT_MIN = -90.0;
export const WGS84_LAT_MAX = 90.0;

export const GEO_STEP_MAX = 26; // 26 bits per axis -> 52-bit integer score (fits IEEE-754 double)
export const EARTH_RADIUS_METERS = 6372797.560856; // Redis quadratic mean Earth radius

const DEG_TO_RAD = Math.PI / 180.0;
const RAD_TO_DEG = 180.0 / Math.PI;
const TWO_POW_26 = 67108864; // 2^26
const BASE32_ALPHABET = '0123456789bcdefghjkmnpqrstuvwxyz';

export type GeoUnit = 'm' | 'km' | 'ft' | 'mi';

export interface GeoCoordinates {
  longitude: number;
  latitude: number;
}

export interface GeoScoreWindow {
  minScore: number;
  maxScore: number;
}

/**
 * Validates longitude and latitude against Redis EPSG:3857 Mercator limits.
 */
export function validateCoordinates(longitude: number, latitude: number): void {
  if (
    !Number.isFinite(longitude) ||
    !Number.isFinite(latitude) ||
    longitude < GEO_LON_MIN ||
    longitude > GEO_LON_MAX ||
    latitude < GEO_LAT_MIN ||
    latitude > GEO_LAT_MAX
  ) {
    throw new Error(`ERR invalid longitude,latitude pair ${longitude},${latitude}`);
  }
}

/**
 * Parses and normalizes a distance unit token ('m', 'km', 'ft', 'mi').
 */
export function parseGeoUnit(raw: string): GeoUnit {
  const lower = raw.toLowerCase();
  if (lower === 'm' || lower === 'km' || lower === 'ft' || lower === 'mi') {
    return lower;
  }
  throw new Error('ERR unsupported unit provided. please use M, KM, FT, MI');
}

/**
 * Converts distance from given GeoUnit to meters.
 */
export function toMeters(value: number, unit: GeoUnit): number {
  switch (unit) {
    case 'm':
      return value;
    case 'km':
      return value * 1000.0;
    case 'ft':
      return value * 0.3048;
    case 'mi':
      return value * 1609.344;
  }
}

/**
 * Converts distance from meters to target GeoUnit.
 */
export function fromMeters(meters: number, unit: GeoUnit): number {
  switch (unit) {
    case 'm':
      return meters;
    case 'km':
      return meters / 1000.0;
    case 'ft':
      return meters / 0.3048;
    case 'mi':
      return meters / 1609.344;
  }
}

/**
 * Spreads the lowest 26 bits of an unsigned integer into even bit positions (0, 2, 4, ..., 50).
 */
function spreadBits26(v: number): bigint {
  let x = BigInt(v >>> 0) & 0x03ffffffn;
  x = (x | (x << 16n)) & 0x0000ffff0000ffffn;
  x = (x | (x << 8n)) & 0x00ff00ff00ff00ffn;
  x = (x | (x << 4n)) & 0x0f0f0f0f0f0f0f0fn;
  x = (x | (x << 2n)) & 0x3333333333333333n;
  x = (x | (x << 1n)) & 0x5555555555555555n;
  return x;
}

/**
 * Compacts even bit positions (0, 2, 4, ..., 50) of a 52-bit BigInt back into a 26-bit integer.
 */
function compactBits26(v: bigint): number {
  let x = v & 0x5555555555555555n;
  x = (x | (x >> 1n)) & 0x3333333333333333n;
  x = (x | (x >> 2n)) & 0x0f0f0f0f0f0f0f0fn;
  x = (x | (x >> 4n)) & 0x00ff00ff00ff00ffn;
  x = (x | (x >> 8n)) & 0x0000ffff0000ffffn;
  x = (x | (x >> 16n)) & 0x0000000003ffffffn;
  return Number(x);
}

/**
 * Interleaves `step`-bit quantized longitude (odd bits, MSB) and latitude (even bits, LSB).
 */
export function interleaveBits(lonBits: number, latBits: number): bigint {
  return (spreadBits26(lonBits) << 1n) | spreadBits26(latBits);
}

/**
 * Encodes (longitude, latitude) into a 52-bit Morton-interleaved Geohash integer score.
 */
export function encodeGeohash52(longitude: number, latitude: number): number {
  validateCoordinates(longitude, latitude);

  const lonNorm = (longitude - GEO_LON_MIN) / (GEO_LON_MAX - GEO_LON_MIN);
  const latNorm = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

  const lonBits = Math.min(TWO_POW_26 - 1, Math.max(0, Math.floor(lonNorm * TWO_POW_26)));
  const latBits = Math.min(TWO_POW_26 - 1, Math.max(0, Math.floor(latNorm * TWO_POW_26)));

  return Number(interleaveBits(lonBits, latBits));
}

/**
 * Decodes a 52-bit Geohash integer score back to cell-centered (longitude, latitude).
 */
export function decodeGeohash52(hash52: number): GeoCoordinates {
  const bits = BigInt(Math.trunc(hash52));
  const latBits = compactBits26(bits);
  const lonBits = compactBits26(bits >> 1n);

  const longitude =
    GEO_LON_MIN + ((lonBits + 0.5) / TWO_POW_26) * (GEO_LON_MAX - GEO_LON_MIN);
  const latitude =
    GEO_LAT_MIN + ((latBits + 0.5) / TWO_POW_26) * (GEO_LAT_MAX - GEO_LAT_MIN);

  return { longitude, latitude };
}

/**
 * Encodes (longitude, latitude) into a standard 11-character WGS-84 Base32 Geohash string.
 */
export function encodeGeohashBase32(
  longitude: number,
  latitude: number,
  precision: number = 11
): string {
  let minLon = GEO_LON_MIN;
  let maxLon = GEO_LON_MAX;
  let minLat = WGS84_LAT_MIN;
  let maxLat = WGS84_LAT_MAX;

  let isEvenBit = true;
  let bitCount = 0;
  let chIndex = 0;
  let out = '';

  while (out.length < precision) {
    if (isEvenBit) {
      const mid = (minLon + maxLon) * 0.5;
      if (longitude >= mid) {
        chIndex = (chIndex << 1) | 1;
        minLon = mid;
      } else {
        chIndex = chIndex << 1;
        maxLon = mid;
      }
    } else {
      const mid = (minLat + maxLat) * 0.5;
      if (latitude >= mid) {
        chIndex = (chIndex << 1) | 1;
        minLat = mid;
      } else {
        chIndex = chIndex << 1;
        maxLat = mid;
      }
    }

    isEvenBit = !isEvenBit;
    bitCount++;

    if (bitCount === 5) {
      out += BASE32_ALPHABET[chIndex];
      bitCount = 0;
      chIndex = 0;
    }
  }

  return out;
}

/**
 * Computes great-circle distance in meters between two geographic points using Haversine formula.
 */
export function haversineDistanceMeters(
  lon1: number,
  lat1: number,
  lon2: number,
  lat2: number
): number {
  const dLatRad = (lat2 - lat1) * DEG_TO_RAD;
  const dLonRad = (lon2 - lon1) * DEG_TO_RAD;
  const lat1Rad = lat1 * DEG_TO_RAD;
  const lat2Rad = lat2 * DEG_TO_RAD;

  const sinHalfLat = Math.sin(dLatRad * 0.5);
  const sinHalfLon = Math.sin(dLonRad * 0.5);

  const a =
    sinHalfLat * sinHalfLat +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) * sinHalfLon * sinHalfLon;
  const c = 2.0 * Math.asin(Math.min(1.0, Math.sqrt(a)));
  return EARTH_RADIUS_METERS * c;
}

/**
 * Evaluates whether target (lon, lat) lies inside a geodesic width x height bounding box
 * centered at (centerLon, centerLat), matching Redis geohash_helper box containment.
 */
export function isWithinBoundingBox(
  centerLon: number,
  centerLat: number,
  targetLon: number,
  targetLat: number,
  widthMeters: number,
  heightMeters: number
): boolean {
  const halfHeight = heightMeters * 0.5;
  const halfWidth = widthMeters * 0.5;

  // North-South meridional distance along center longitude
  const latDist = haversineDistanceMeters(centerLon, centerLat, centerLon, targetLat);
  if (latDist > halfHeight) return false;

  // East-West parallel distance evaluated at center latitude
  const lonDist = haversineDistanceMeters(centerLon, centerLat, targetLon, centerLat);
  if (lonDist > halfWidth) return false;

  return true;
}

/**
 * Computes merged 52-bit Geohash score intervals covering the 9-cell Morton neighborhood
 * around (centerLon, centerLat) at the finest grid step that strictly encloses
 * a search region of dimensions (widthMeters x heightMeters).
 */
export function computeGeohashSearchWindows(
  centerLon: number,
  centerLat: number,
  widthMeters: number,
  heightMeters: number
): GeoScoreWindow[] {
  const MAX_52 = Number((1n << 52n) - 1n);
  const halfW = Math.max(0.5, widthMeters * 0.5);
  const halfH = Math.max(0.5, heightMeters * 0.5);

  const latDeltaDeg = (halfH / EARTH_RADIUS_METERS) * RAD_TO_DEG;
  const maxAbsLat = Math.min(GEO_LAT_MAX, Math.abs(centerLat) + latDeltaDeg);
  const cosLat = Math.max(0.01, Math.cos(maxAbsLat * DEG_TO_RAD));

  const lonSpanDeg = GEO_LON_MAX - GEO_LON_MIN; // 360
  const latSpanDeg = GEO_LAT_MAX - GEO_LAT_MIN; // ~170.10225756

  // Select highest grid step s in [0..26] where a single cell exceeds halfW x halfH (with 5% safety margin)
  let step = 0;
  for (let s = 1; s <= GEO_STEP_MAX; s++) {
    const gridCount = Math.pow(2, s);
    const cellHeightMeters = EARTH_RADIUS_METERS * ((latSpanDeg / gridCount) * DEG_TO_RAD);
    const cellWidthMeters =
      EARTH_RADIUS_METERS * ((lonSpanDeg / gridCount) * DEG_TO_RAD) * cosLat;

    if (cellHeightMeters >= halfH * 1.05 && cellWidthMeters >= halfW * 1.05) {
      step = s;
    } else {
      break;
    }
  }

  if (step <= 1) {
    return [{ minScore: 0, maxScore: MAX_52 }];
  }

  const gridSize = 1 << step;
  const lonNorm = (centerLon - GEO_LON_MIN) / lonSpanDeg;
  const latNorm = (centerLat - GEO_LAT_MIN) / latSpanDeg;

  const centerX = Math.min(gridSize - 1, Math.max(0, Math.floor(lonNorm * gridSize)));
  const centerY = Math.min(gridSize - 1, Math.max(0, Math.floor(latNorm * gridSize)));

  const shiftBits = BigInt((GEO_STEP_MAX - step) * 2);
  const cellSpan = 1n << shiftBits;
  const rawWindows: GeoScoreWindow[] = [];

  for (let dx = -1; dx <= 1; dx++) {
    // Wrap longitude cells across the -180 / +180 antimeridian
    const nx = (centerX + dx + gridSize) % gridSize;
    for (let dy = -1; dy <= 1; dy++) {
      const ny = centerY + dy;
      if (ny < 0 || ny >= gridSize) continue;

      const prefix = interleaveBits(nx, ny);
      const minHash = prefix << shiftBits;
      const maxHash = minHash + cellSpan - 1n;
      rawWindows.push({
        minScore: Number(minHash),
        maxScore: Number(maxHash),
      });
    }
  }

  // Sort and merge overlapping or adjacent 52-bit score windows
  rawWindows.sort((a, b) => a.minScore - b.minScore);
  const merged: GeoScoreWindow[] = [];

  for (const win of rawWindows) {
    if (merged.length === 0) {
      merged.push({ ...win });
    } else {
      const prev = merged[merged.length - 1];
      if (win.minScore <= prev.maxScore + 1) {
        prev.maxScore = Math.max(prev.maxScore, win.maxScore);
      } else {
        merged.push({ ...win });
      }
    }
  }

  return merged;
}
