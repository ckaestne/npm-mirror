import ChangesStream from 'changes-stream';
import { writeFile, mkdir, access } from 'node:fs/promises';
import { createWriteStream, writeFileSync, readFileSync } from 'node:fs';
import * as path from 'path';
import normalize from 'normalize-registry-metadata'
import request from 'request'
import Queue from 'better-queue'
import { Counter, Gauge, collectDefaultMetrics, register } from 'prom-client'
import express from 'express';



// initializing
import config from './config.json';
try {
  const seq_store = JSON.parse(readFileSync(config.update_seq_store).toString())
  if (seq_store && seq_store.update_seq && (seq_store.update_seq > config.update_seq)) {
    console.log(`Starting with stored ${seq_store.update_seq} rather than config's ${config.update_seq} seq`)
    config.update_seq = seq_store.update_seq
  }
} catch (e) { }
console.log(`Tracking changes to ${config.couchdb} from ${config.update_seq}`)

// metrics / monitoring
let npmUpdateCounter = new Counter({ name: "npmmirror_npm_update_counter", help: "number of npm updates processed" })
let downloadQueueLength = new Gauge({ name: "npmmirror_download_queue_length", help: "length of the download queue" })
collectDefaultMetrics({ prefix: "npmmirror_" })
let app = express()
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  } catch (ex) {
    res.status(500).end(ex);
  }
});
app.listen(8080, () => console.log(`Metrics listening on port 8080.`));


// subscribing to npm changes
var changes = new ChangesStream({
  db: config.couchdb,
  include_docs: true,
  since: config.update_seq
});

// processing changes
changes.on('data', async function (change: any) {
  npmUpdateCounter.inc()

  normalize(change)
  if (change.deleted) {
    await writeDeletion(new Date(), change.id)
  } else {
    await writeUpdate(new Date(), change.id, JSON.stringify(change.doc))
    // await downloadLatestPkg(change.id, change.doc)
  }

  // keeping last processed id
  writeFileSync(config.update_seq_store, `{"update_seq":${change.seq}}`)
});

changes.on('error', function (e) {
  console.log(e);
});





function pkgDir(pkgId: string) {
  return path.join(config.targetDir, pkgId)
}

async function writeUpdate(time: Date, pkgId: string, json: string) {
  console.log(`updated ${pkgId}`)

  const d = pkgDir(pkgId)
  await mkdir(d, { recursive: true })

  const p = path.join(d, time.toISOString() + ".json")
  await writeFile(p, json)
}

async function writeDeletion(time: Date, pkgId: string) {
  console.log(`deleted ${pkgId}`)

  const d = pkgDir(pkgId)
  await mkdir(d, { recursive: true })
  const p = path.join(d, time.toISOString() + "-DELETED.json")
  await writeFile(p, "")
}

async function downloadLatestPkg(pkgId: string, doc: any) {
  let l = doc['dist-tags'] && doc['dist-tags'].latest
  if (!l) { console.log(`no latest version for ${doc._id}`); return }
  let v = doc.versions && doc.versions[l]
  if (!v) { console.log(`latest version ${l} for ${doc._id} not found`); return }
  let tar = v.dist && v.dist.tarball
  if (!tar) { console.log(`no tarball for version ${v} for ${doc._id}`); return }
  let size = v.dist && v.dist.unpackedSize
  if (!size) { console.log(`no size for ${tar} for ${doc._id}`); return }


  let filename = path.basename(tar)
  const d = pkgDir(pkgId)
  await mkdir(d, { recursive: true })

  const p = path.join(d, filename)

  const a = access(p)

  a.catch((r) => {
    if (size > config.max_tar_size) { console.log(`skipping download of ${tar} for ${doc._id}, too big (${(size / 1024 / 1024).toFixed(1)}mb > max size of ${config.max_tar_size / 1024 / 1024}mb)`); return true }

    downloadQueue.push([p, tar])

    return true
  }).then((v) => {
    if (!v) console.log(`already downloaded ${tar}`)
  })
}


function processDownload(input: [string, string], cb) {
  let [p, tar] = input
  console.log(`downloading ${tar}`)
  let file = createWriteStream(p);
  request(tar).
    pipe(file).
    on('finish', () => { console.log(`downloading ${tar} finished`); cb(null, p) }).
    on('error', (e) => { console.log(`downloading ${tar} failed ${e}`); cb(e, null) })
}

let downloadQueue: any = new Queue(processDownload, {
  concurrent: 1, maxRetries: 1, store: {
    type: 'sql',
    dialect: 'sqlite',
    path: config.download_queue_store
  }
}).
  on('task_queued', () => { downloadQueueLength.set(downloadQueue.length) }).
  on('task_finish', () => { downloadQueueLength.set(downloadQueue.length) }).
  on('task_failed', () => { downloadQueueLength.set(downloadQueue.length) })

