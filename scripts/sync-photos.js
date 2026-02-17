const fs = require('fs');
const path = require('path');
const sharp = require('sharp');

const CONTAINER = 'iCloud.lucaspfeiffer.sun';
const ENVIRONMENT = 'production';
const API_BASE = `https://api.apple-cloudkit.com/database/1/${CONTAINER}/${ENVIRONMENT}/public`;
const API_TOKEN = process.env.CLOUDKIT_API_TOKEN;

const PHOTOS_DIR = path.join(__dirname, '..', 'photos');
const THUMBNAILS_DIR = path.join(PHOTOS_DIR, 'thumbnails');
const FULL_DIR = path.join(PHOTOS_DIR, 'full');
const MANIFEST_PATH = path.join(__dirname, '..', 'photos.json');

async function queryCloudKit(continuationMarker) {
    const body = {
        query: {
            recordType: 'SunPhoto',
            filterBy: [{
                fieldName: 'status',
                comparator: 'EQUALS',
                fieldValue: { value: 'approved', type: 'STRING' }
            }]
        },
        resultsLimit: 100
    };

    if (continuationMarker) {
        body.continuationMarker = continuationMarker;
    }

    const url = `${API_BASE}/records/query?ckAPIToken=${API_TOKEN}`;
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body)
    });

    if (!res.ok) {
        throw new Error(`CloudKit query failed: ${res.status} ${await res.text()}`);
    }

    return res.json();
}

async function fetchAllRecords() {
    const allRecords = [];
    let continuationMarker = null;

    do {
        const data = await queryCloudKit(continuationMarker);
        if (data.records) {
            allRecords.push(...data.records);
        }
        continuationMarker = data.continuationMarker || null;
    } while (continuationMarker);

    return allRecords;
}

async function downloadAndConvert(url, outputPath, quality) {
    const res = await fetch(url);
    if (!res.ok) {
        throw new Error(`Failed to download ${url}: ${res.status}`);
    }
    const buffer = Buffer.from(await res.arrayBuffer());
    await sharp(buffer).webp({ quality }).toFile(outputPath);
}

function loadManifest() {
    if (fs.existsSync(MANIFEST_PATH)) {
        return JSON.parse(fs.readFileSync(MANIFEST_PATH, 'utf-8'));
    }
    return [];
}

function saveManifest(photos) {
    // Sort by captureDate descending
    photos.sort((a, b) => b.captureDate - a.captureDate);
    fs.writeFileSync(MANIFEST_PATH, JSON.stringify(photos, null, 2) + '\n');
}

async function main() {
    if (!API_TOKEN) {
        console.error('CLOUDKIT_API_TOKEN environment variable is required');
        process.exit(1);
    }

    // Ensure directories exist
    fs.mkdirSync(THUMBNAILS_DIR, { recursive: true });
    fs.mkdirSync(FULL_DIR, { recursive: true });

    console.log('Fetching approved photos from CloudKit...');
    const records = await fetchAllRecords();
    console.log(`Found ${records.length} approved photos`);

    const existingManifest = loadManifest();
    const existingIds = new Set(existingManifest.map(p => p.id));
    const cloudKitIds = new Set(records.map(r => r.recordName));

    // Remove photos that are no longer in CloudKit
    const removedPhotos = existingManifest.filter(p => !cloudKitIds.has(p.id));
    for (const photo of removedPhotos) {
        console.log(`Removing photo: ${photo.id}`);
        const thumbPath = path.join(__dirname, '..', photo.thumbnail);
        const fullPath = path.join(__dirname, '..', photo.image);
        if (fs.existsSync(thumbPath)) fs.unlinkSync(thumbPath);
        if (fs.existsSync(fullPath)) fs.unlinkSync(fullPath);
    }

    // Process new photos
    const newPhotos = [];
    for (const record of records) {
        const id = record.recordName;

        if (existingIds.has(id)) {
            // Keep existing entry
            newPhotos.push(existingManifest.find(p => p.id === id));
            continue;
        }

        const fields = record.fields;
        const thumbnailURL = fields.thumbnail?.value?.downloadURL;
        const imageURL = fields.image?.value?.downloadURL;
        const locationName = fields.locationName?.value || 'Unknown';
        const captureDate = fields.captureDate?.value || Date.now();

        if (!thumbnailURL || !imageURL) {
            console.warn(`Skipping ${id}: missing thumbnail or image URL`);
            continue;
        }

        const thumbPath = path.join(THUMBNAILS_DIR, `${id}.webp`);
        const fullPath = path.join(FULL_DIR, `${id}.webp`);

        console.log(`Processing new photo: ${id} (${locationName})`);

        try {
            await downloadAndConvert(thumbnailURL, thumbPath, 80);
            await downloadAndConvert(imageURL, fullPath, 90);

            newPhotos.push({
                id,
                locationName,
                captureDate,
                thumbnail: `photos/thumbnails/${id}.webp`,
                image: `photos/full/${id}.webp`
            });
        } catch (err) {
            console.error(`Failed to process ${id}:`, err.message);
        }
    }

    saveManifest(newPhotos);
    console.log(`Manifest written with ${newPhotos.length} photos`);
}

main().catch(err => {
    console.error('Sync failed:', err);
    process.exit(1);
});
