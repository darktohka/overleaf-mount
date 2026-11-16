import fs from 'node:fs';
import path from 'node:path';
import { RemoteClient } from './remote_client.mjs';

const MOUNT_DIR = process.argv[2] || '/tmp/m1';
const PROJECT_ID = '6afacb5e0082f5af0cb6d6f7';
const MAIN_TEX_DOC_ID = '6afacb5e0082f5af0cb6d6fc';
const SAMPLE_BIB_DOC_ID = '6afacb5e0082f5af0cb6d703';

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function assert(condition, message) {
  if (!condition) {
    console.error(`\x1b[31m[FAIL]\x1b[0m ${message}`);
    process.exit(1);
  }
  console.log(`\x1b[32m[ASSERT OK]\x1b[0m ${message}`);
}

function makeReplaceOp(docText, target, replacement) {
  const p = docText.indexOf(target);
  if (p === -1) throw new Error(`Target "${target}" not found in docText!`);
  return [
    { p, d: target },
    { p, i: replacement }
  ];
}

async function main() {
  console.log('====================================================');
  console.log('   Overleaf FUSE Filesystem Automated Verification  ');
  console.log('====================================================');
  console.log(`Mount Directory: ${MOUNT_DIR}`);

  const remote = new RemoteClient();
  await remote.login();
  await remote.connectWs(PROJECT_ID);
  console.log(`Remote collaborator connected (Public ID: ${remote.publicId})`);

  // Verify mount is active and readable
  assert(fs.existsSync(MOUNT_DIR), `Mount point ${MOUNT_DIR} exists`);
  const initialFiles = fs.readdirSync(MOUNT_DIR);
  console.log(`Initial files in mount: ${initialFiles.join(', ')}`);
  assert(initialFiles.includes('main.tex'), 'main.tex exists in mount');
  assert(initialFiles.includes('sample.bib'), 'sample.bib exists in mount');
  assert(initialFiles.includes('frog.jpg'), 'frog.jpg binary file exists in mount');

  // =========================================================================
  // SCENARIO 1: Local FUSE Write -> Overleaf Real-Time Synchronization
  // =========================================================================
  console.log('\n--- SCENARIO 1: Local FUSE Write -> Overleaf Sync ---');
  const s1Content = `\\documentclass{article}
\\begin{document}
Scenario 1: Verified Local Write to Overleaf Synchronization.
Timestamp: ${Date.now()}
\\end{document}
`;
  const mainTexPath = path.join(MOUNT_DIR, 'main.tex');
  fs.writeFileSync(mainTexPath, s1Content);
  console.log('Written new content to main.tex via FUSE');
  await sleep(1500);

  const docAfterS1 = await remote.joinDoc(MAIN_TEX_DOC_ID);
  const overleafS1Text = docAfterS1.lines.join('\n');
  console.log('overleafS1Text:', JSON.stringify(overleafS1Text));
  assert(
    overleafS1Text.includes('Scenario 1: Verified Local Write to Overleaf Synchronization.'),
    'Overleaf project received and stored the content written via FUSE'
  );
  console.log('\x1b[32m[PASS] Scenario 1 completed successfully.\x1b[0m');

  // =========================================================================
  // SCENARIO 2: Remote Collaborator Edit -> FUSE Real-Time Synchronization
  // =========================================================================
  console.log('\n--- SCENARIO 2: Remote Collaborator Edit -> FUSE Real-Time Sync ---');
  const remoteBibEntry = `@article{remote2026,
  title={Collaborator Realtime Edit},
  author={Overleaf Remote Collaborator},
  year={2026}
}
`;
  const bibDoc = await remote.joinDoc(SAMPLE_BIB_DOC_ID);
  const currentBibText = bibDoc.lines.join('\n');
  // Send remote OT insert at the top of sample.bib
  const insertOp = [{ p: 0, i: remoteBibEntry }];
  await remote.sendOt(SAMPLE_BIB_DOC_ID, bibDoc.version, insertOp);
  console.log(`Remote collaborator sent OT update to sample.bib (v: ${bibDoc.version})`);
  await sleep(1500);

  const fuseBibContent = fs.readFileSync(path.join(MOUNT_DIR, 'sample.bib'), 'utf-8');
  assert(
    fuseBibContent.includes('Collaborator Realtime Edit'),
    'FUSE filesystem immediately received and synchronized the remote collaborator OT update'
  );
  console.log('\x1b[32m[PASS] Scenario 2 completed successfully.\x1b[0m');

  // =========================================================================
  // SCENARIO 3: Concurrent Non-Conflicting Edits Reconciled
  // =========================================================================
  console.log('\n--- SCENARIO 3: Concurrent Non-Conflicting Edits Reconciled ---');
  const baseS3 = `\\section{Introduction}
Section 1: Original introduction.

\\section{Methodology}
Section 2: Original methodology.
`;
  // First set base document state
  fs.writeFileSync(mainTexPath, baseS3);
  await sleep(1500);
  const currentDocS3 = await remote.joinDoc(MAIN_TEX_DOC_ID);
  const vS3 = currentDocS3.version;

  // Remote updates Section 2
  const remoteOpS3 = makeReplaceOp(
    baseS3,
    'Original methodology.',
    'Remote collaborator enhanced experimental methodology.'
  );

  // Simultaneously, local FUSE updates Section 1
  const localUpdatedS3 = baseS3.replace(
    'Original introduction.',
    'Local FUSE author comprehensive introduction.'
  );

  // Fire both concurrently!
  console.log('Triggering concurrent edits to Section 1 (local) and Section 2 (remote)...');
  await Promise.all([
    remote.sendOt(MAIN_TEX_DOC_ID, vS3, remoteOpS3),
    (async () => {
      fs.writeFileSync(mainTexPath, localUpdatedS3);
    })()
  ]);

  await sleep(2000);

  const localTextS3 = fs.readFileSync(mainTexPath, 'utf-8');
  const overleafDocS3 = await remote.joinDoc(MAIN_TEX_DOC_ID);
  const overleafTextS3 = overleafDocS3.lines.join('\n');

  console.log(`FUSE main.tex after reconciliation:\n${localTextS3}`);
  assert(
    localTextS3.includes('Local FUSE author comprehensive introduction'),
    'FUSE contains local Section 1 edit'
  );
  assert(
    localTextS3.includes('Remote collaborator enhanced experimental methodology'),
    'FUSE contains reconciled remote Section 2 edit'
  );
  console.log('\x1b[32m[PASS] Scenario 3 completed successfully.\x1b[0m');

  // =========================================================================
  // SCENARIO 4: Concurrent Conflicting Edits (Our Version Authoritative)
  // =========================================================================
  console.log('\n--- SCENARIO 4: Concurrent Conflicting Edits (Our Version Authoritative) ---');
  const baseS4 = `\\section{Conclusion}
The proven result is that Algorithm Alpha is the most efficient choice.
`;
  fs.writeFileSync(mainTexPath, baseS4);
  await sleep(1500);
  const docS4 = await remote.joinDoc(MAIN_TEX_DOC_ID);
  const vS4 = docS4.version;

  // Remote conflicting edit to the EXACT same sentence:
  const remoteOpS4 = makeReplaceOp(
    baseS4,
    'Algorithm Alpha is the most efficient choice.',
    'Algorithm Beta is clearly superior to Alpha.'
  );

  // Local FUSE conflicting edit to the EXACT same sentence:
  const localS4 = baseS4.replace(
    'Algorithm Alpha is the most efficient choice.',
    'Algorithm Gamma is conclusively the undisputed winner.'
  );

  console.log('Triggering CONFLICTING edits to the exact same sentence simultaneously...');
  console.log('Remote edit: "Algorithm Beta is clearly superior to Alpha."');
  console.log('Local edit:  "Algorithm Gamma is conclusively the undisputed winner."');

  await Promise.all([
    remote.sendOt(MAIN_TEX_DOC_ID, vS4, remoteOpS4),
    (async () => {
      fs.writeFileSync(mainTexPath, localS4);
    })()
  ]);

  await sleep(2500);

  const localTextS4 = fs.readFileSync(mainTexPath, 'utf-8');
  const overleafDocS4 = await remote.joinDoc(MAIN_TEX_DOC_ID);
  const overleafTextS4 = overleafDocS4.lines.join('\n');

  console.log(`FUSE main.tex content: ${localTextS4.trim()}`);
  console.log(`Overleaf main.tex content: ${overleafTextS4.trim()}`);

  assert(
    localTextS4.includes('Algorithm Gamma is conclusively the undisputed winner'),
    'Requirement proven: Our version was kept authoritative in FUSE on conflict!'
  );
  assert(
    !localTextS4.includes('Algorithm Beta'),
    'Requirement proven: Conflicting remote version was overridden by our authoritative version!'
  );
  assert(
    overleafTextS4.includes('Algorithm Gamma is conclusively the undisputed winner'),
    'Requirement proven: Corrective OT propagated our authoritative version to Overleaf server!'
  );
  console.log('\x1b[32m[PASS] Scenario 4 completed successfully: Authoritative conflict resolution verified!\x1b[0m');

  // =========================================================================
  // SCENARIO 5: Filesystem Structure Operations (Create, Subdir, Rename, Delete)
  // =========================================================================
  console.log('\n--- SCENARIO 5: Filesystem Structure Operations ---');
  const newFilePath = path.join(MOUNT_DIR, 'chapter1.tex');
  fs.writeFileSync(newFilePath, '\\section{Chapter 1}\nInitial chapter text.');
  await sleep(1500);
  assert(fs.existsSync(newFilePath), 'Created file chapter1.tex exists in FUSE mount');
  const chapter1Content = fs.readFileSync(newFilePath, 'utf-8');
  assert(chapter1Content.includes('Initial chapter text'), 'Can read back created file content');

  const subfolderPath = path.join(MOUNT_DIR, 'sections');
  fs.mkdirSync(subfolderPath);
  await sleep(1500);
  assert(fs.existsSync(subfolderPath), 'Created directory sections exists in FUSE mount');

  const renamedPath = path.join(subfolderPath, 'intro.tex');
  fs.renameSync(newFilePath, renamedPath);
  await sleep(1500);
  assert(!fs.existsSync(newFilePath), 'Old file path no longer exists after rename');
  assert(fs.existsSync(renamedPath), 'Renamed file exists in new location sections/intro.tex');

  fs.unlinkSync(renamedPath);
  await sleep(1000);
  assert(!fs.existsSync(renamedPath), 'File deleted successfully');

  fs.rmdirSync(subfolderPath);
  await sleep(1000);
  assert(!fs.existsSync(subfolderPath), 'Directory removed successfully');

  // Verify binary file frog.jpg can be read and matches original size
  const frogStat = fs.statSync(path.join(MOUNT_DIR, 'frog.jpg'));
  assert(frogStat.size > 50000, `Binary file frog.jpg readable with size ${frogStat.size} bytes`);

  console.log('\x1b[32m[PASS] Scenario 5 completed successfully.\x1b[0m');

  // =========================================================================
  // SCENARIO 6: .gitignore Filtering & In-Memory Only Retention
  // =========================================================================
  console.log('\n--- SCENARIO 6: .gitignore Filtering & In-Memory Only Retention ---');

  // 1. Write .gitignore
  const gitignorePath = path.join(MOUNT_DIR, '.gitignore');
  const gitignoreContent = `*.log\nbuild/\ntemp/\n!keep.log\n`;
  fs.writeFileSync(gitignorePath, gitignoreContent);
  await sleep(1500);

  // 2. Create ignored files and directories
  const testLogPath = path.join(MOUNT_DIR, 'test.log');
  fs.writeFileSync(testLogPath, 'Debug log entry 1\n');

  const buildDirPath = path.join(MOUNT_DIR, 'build');
  fs.mkdirSync(buildDirPath);

  const buildArtifactPath = path.join(buildDirPath, 'output.pdf');
  fs.writeFileSync(buildArtifactPath, 'Fake binary pdf content');

  // 3. Create whitelisted file that would otherwise match *.log
  const keepLogPath = path.join(MOUNT_DIR, 'keep.log');
  fs.writeFileSync(keepLogPath, 'Important whitelisted log entry\n');

  // 4. Create standard unignored doc
  const notesPath = path.join(MOUNT_DIR, 'notes.tex');
  fs.writeFileSync(notesPath, '\\section{Notes}\nSome project notes.');

  await sleep(2000);

  // 5. Verify local filesystem state: ALL files must exist and be fully readable/writable in memory!
  assert(fs.existsSync(testLogPath), 'Ignored file test.log exists locally in FUSE mount');
  assert(fs.readFileSync(testLogPath, 'utf-8') === 'Debug log entry 1\n', 'Can read test.log locally');

  assert(fs.existsSync(buildDirPath), 'Ignored directory build exists locally in FUSE mount');
  assert(fs.existsSync(buildArtifactPath), 'File inside ignored directory exists locally');
  assert(fs.readFileSync(buildArtifactPath, 'utf-8') === 'Fake binary pdf content', 'Can read file inside ignored directory');

  assert(fs.existsSync(keepLogPath), 'Whitelisted file keep.log exists locally in FUSE mount');
  assert(fs.existsSync(notesPath), 'Standard file notes.tex exists locally in FUSE mount');

  // 6. Append to ignored file locally (must succeed and stay local)
  fs.appendFileSync(testLogPath, 'Debug log entry 2\n');
  assert(fs.readFileSync(testLogPath, 'utf-8') === 'Debug log entry 1\nDebug log entry 2\n', 'Appended to ignored file locally');

  // 7. Verify Overleaf state via fresh project tree query:
  // - .gitignore MUST be on Overleaf
  // - notes.tex MUST be on Overleaf
  // - keep.log MUST be on Overleaf (whitelisted)
  // - test.log MUST NOT be on Overleaf!
  // - build directory MUST NOT be on Overleaf!
  const freshProj = await remote.getFreshProject(PROJECT_ID);
  const overleafDocNames = freshProj.rootFolder[0].docs.map(d => d.name);
  const overleafFolderNames = freshProj.rootFolder[0].folders.map(f => f.name);

  console.log(`Overleaf docs present: ${overleafDocNames.join(', ')}`);
  console.log(`Overleaf folders present: ${overleafFolderNames.join(', ')}`);

  assert(overleafDocNames.includes('.gitignore'), '.gitignore was synchronized to Overleaf');
  assert(overleafDocNames.includes('notes.tex'), 'Unignored notes.tex was synchronized to Overleaf');
  assert(overleafDocNames.includes('keep.log'), 'Whitelisted keep.log was synchronized to Overleaf');

  assert(!overleafDocNames.includes('test.log'), 'Requirement proven: Ignored file test.log was NOT synchronized to Overleaf!');
  assert(!overleafFolderNames.includes('build'), 'Requirement proven: Ignored folder build was NOT synchronized to Overleaf!');

  // 8. Delete ignored files locally: must succeed cleanly without errors
  fs.unlinkSync(testLogPath);
  assert(!fs.existsSync(testLogPath), 'test.log removed from local FUSE');

  fs.unlinkSync(buildArtifactPath);
  fs.rmdirSync(buildDirPath);
  assert(!fs.existsSync(buildDirPath), 'build directory removed from local FUSE');

  // Clean up notes and keep.log and .gitignore
  fs.unlinkSync(notesPath);
  fs.unlinkSync(keepLogPath);
  fs.unlinkSync(gitignorePath);
  await sleep(1500);

  console.log('\x1b[32m[PASS] Scenario 6 completed successfully: .gitignore filtering and in-memory retention verified!\x1b[0m');

  remote.close();
  console.log('\n====================================================');
  console.log('   ALL SCENARIOS PASSED WITH COMPLETE VERIFICATION  ');
  console.log('====================================================');
  process.exit(0);
}

main().catch(err => {
  console.error('Test suite error:', err);
  process.exit(1);
});
