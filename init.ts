import ChangesStream from 'changes-stream';
import { writeFile, mkdir, access } from 'node:fs/promises';
import { createWriteStream, writeFileSync, readFileSync, createReadStream } from 'node:fs';
import * as path from 'path';
import normalize from 'normalize-registry-metadata'
import request from 'request'
import Queue from 'better-queue'
import { Counter, Gauge, collectDefaultMetrics, register } from 'prom-client'
import express from 'express';
import JSONStream from 'JSONStream'




// initializing
import config from './config.json';
import { argv } from 'node:process';
console.log(`Initializing from ${argv[2]}`)
let date = new Date(2022,10,0)

// metrics / monitoring
let npmUpdateCounter = new Counter({ name: "npmmirror_init_counter", help: "number of npm updates processed" })
let downloadQueueLength = new Gauge({ name: "npmmirror_init_download_queue_length", help: "length of the download queue" })
collectDefaultMetrics({ prefix: "npmmirror_init_" })
let app = express()
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  } catch (ex) {
    res.status(500).end(ex);
  }
});
app.listen(8081, () => console.log(`Metrics listening on port 8081.`));


// subscribing to npm changes
var stream = JSONStream.parse(["rows", true, "doc"])


// processing changes
stream.on('data', async function (change: any) {
  npmUpdateCounter.inc()

  normalize(change)
  await writeUpdate(date, change.id, JSON.stringify(change))
  await downloadLatestPkg(change.id, change)

  // keeping last processed id
  writeFileSync(config.update_seq_store, `{"update_seq":${change.seq}}`)
});

stream.on('error', function (e) {
  console.log(e);
});

createReadStream('npmjs.20210617.json').pipe(stream)





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
  concurrent: 3, maxRetries: 1
}).
  on('task_queued', () => { downloadQueueLength.set(downloadQueue.length) }).
  on('task_finish', () => { downloadQueueLength.set(downloadQueue.length) }).
  on('task_failed', () => { downloadQueueLength.set(downloadQueue.length) })

