const baseUrl = (process.env.DAEDALMAP_API_BASE_URL || "https://app.daedalmap.com").replace(/\/$/, "");
const packId = process.env.DAEDALMAP_PACK_ID || "earthquakes";

async function readJson(response) {
  const text = await response.text();
  try {
    return JSON.parse(text);
  } catch {
    return { raw: text };
  }
}

const catalogResponse = await fetch(`${baseUrl}/api/v1/catalog`, {
  headers: { Accept: "application/json" },
});
if (!catalogResponse.ok) {
  throw new Error(`Catalog request failed with HTTP ${catalogResponse.status}`);
}

const packResponse = await fetch(`${baseUrl}/api/v1/packs/${encodeURIComponent(packId)}`, {
  headers: { Accept: "application/json" },
});
if (!packResponse.ok) {
  throw new Error(`Pack request failed with HTTP ${packResponse.status}`);
}
const pack = await readJson(packResponse);

const queryResponse = await fetch(`${baseUrl}/api/v1/query/dataset`, {
  method: "POST",
  headers: {
    Accept: "application/json",
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    pack_id: packId,
    limit: 1,
  }),
});
const queryBody = await readJson(queryResponse);

console.log(JSON.stringify({
  base_url: baseUrl,
  pack_id: packId,
  pack,
  query_status: queryResponse.status,
  payment_required: queryResponse.status === 402,
  has_payment_required_header: queryResponse.headers.has("payment-required"),
  response: queryBody,
}, null, 2));
