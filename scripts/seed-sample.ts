/**
 * Seed the ECA (Estonian Competition Authority) database with sample decisions and mergers.
 * Usage: npx tsx scripts/seed-sample.ts [--force]
 */
import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["ECA_DB_PATH"] ?? "data/eca.db";
const force = process.argv.includes("--force");
const dir = dirname(DB_PATH);
if (!existsSync(dir)) { mkdirSync(dir, { recursive: true }); }
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted ${DB_PATH}`); }
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

// --- Sectors ---
const sectors = [
  { id: "energy", name: "Energeetika", name_en: "Energy", description: "Elektrienergeetika, gaasiturundus, soojusenergia ja kaugküte.", decision_count: 2, merger_count: 1 },
  { id: "telecommunications", name: "Telekommunikatsioon", name_en: "Telecommunications", description: "Mobiilside, lairibateenused, kaabeltelevisoon ja interneti infrastruktuur.", decision_count: 2, merger_count: 1 },
  { id: "retail", name: "Jaekaubandus", name_en: "Retail", description: "Toidukauplus, suurkauplused ja jaekaubandusketid.", decision_count: 1, merger_count: 1 },
  { id: "transport", name: "Transport", name_en: "Transport", description: "Raudtee, bussitransport, lennundus ja merendus.", decision_count: 1, merger_count: 0 },
  { id: "financial_services", name: "Finantsteenused", name_en: "Financial services", description: "Pangandus, kindlustus, makseteenused ja kapitaliturud.", decision_count: 1, merger_count: 0 },
];
const insS = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) insS.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

// --- Decisions ---
const decisions = [
  {
    case_number: "5-5/2023", title: "Elering — Elektrienergeetika ülekandevõrgu juurdepääsutingimused",
    date: "2023-05-15", type: "abuse_of_dominance", sector: "energy",
    parties: JSON.stringify(["Elering AS"]),
    summary: "Konkurentsiamet uuris Elering AS-i tegevust seoses juurdepääsutingimustega elektrienergeetika ülekandevõrgule. Elering AS on Eesti ainus põhivõrguettevõtja ja seega reguleeritud monopol. Amet leidis, et Elering kohaldas diskrimineerivaid tingimusi teatud turuosalistele.",
    full_text: "Konkurentsiamet (ECA) alustas menetlust Elering AS-i vastu kaebuse alusel, et ettevõte rakendab elektrienergeetika ülekandevõrgu juurdepääsu suhtes diskrimineerivaid tingimusi. Elering AS on Eesti ainuke põhivõrguettevõtja, kellel on seaduslik monopol ülekandevõrgu opereerimisel. Menetluse käigus tuvastas amet järgmised probleemid: (1) Juurdepääsutasud — Elering kohaldas erinevaid tasumäära erinevate turuosaliste kategooriate suhtes ilma objektiivse põhjenduseta; (2) Ühendamisprotseduurid — uute tootjate ühendamisprotsess oli ebakohane ja põhjustas ebamõistlikke viivitusi; (3) Teabekohustused — Elering ei avaldanud piisavalt teavet ühendamistingimuste ja võimsuse reserveerimise kohta. Konkurentsiamet andis Elering AS-ile ettekirjutuse diskrimineeriva käitumise lõpetamiseks ja juurdepääsutingimuste ühtlustamiseks.",
    outcome: "prohibited", fine_amount: null, gwb_articles: JSON.stringify(["Konkurentsiseadus § 16", "§ 17"]), status: "final",
  },
  {
    case_number: "5-1/2022", title: "Tele2/Elisa — Koostöökokkuleppe hindamine",
    date: "2022-09-01", type: "cartel", sector: "telecommunications",
    parties: JSON.stringify(["Tele2 Eesti AS", "Elisa Eesti AS"]),
    summary: "Konkurentsiamet uuris Tele2 ja Elisa vahelise tugijaamainfrastruktuuri jagamise kokkuleppe vastavust konkurentsieeskirjadele. Amet leidis, et kokkulepe ei piira konkurentsi oluliselt.",
    full_text: "Konkurentsiamet hindas Tele2 Eesti AS-i ja Elisa Eesti AS-i vahelise mobiilsidevõrgu infrastruktuuri jagamise kokkuleppe kooskõla Konkurentsiseaduse §-ga 4. Kokkulepe hõlmas tugijaamade jagamist maapiirkondades 5G teenuse laiendamiseks. Hindamise tulemused: (1) Turuosas mõju — infrastruktuuri jagamine puudutab ainult passiivset infrastruktuuri, mitte aktiivseadmeid; (2) Konkurents säilib — mõlemad operaatorid säilitavad sõltumatu teenuste hinnakujunduse ja klientidega suhtlemise; (3) Sotsiaalne kasu — lairibakontekst laieneb maapiirkondades, mis toetab riiklikku lairibastrateegiat. Amet tuvastas, et kokkulepe ei riku Konkurentsiseaduse §-i 4 ning lubas selle jõusse jääda.",
    outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["Konkurentsiseadus § 4"]), status: "final",
  },
  {
    case_number: "5-3/2023", title: "Coop Eesti — Turgu valitsev seisund jaekaubanduses",
    date: "2023-08-20", type: "abuse_of_dominance", sector: "retail",
    parties: JSON.stringify(["Coop Eesti Keskühistu"]),
    summary: "Uurimine Coop Eesti tegevuse kohta maapiirkondade toidukaubanduses, kus Coopil on kohalik monopol. Amet tuvastas, et teatud tingimustes kohaldab Coop ebamõistlikke hindu.",
    full_text: "Konkurentsiamet uuris Coop Eesti Keskühistu tegevust maapiirkondade jaekaubanduse turgudel. Maapiirkondades on Coop sageli ainus toidupood kättesaadavas vahemikus, mis annab ettevõttele de facto monopoliseisundi. Menetluse käigus analüüsis amet: (1) Hinnatasemeid maapiirkondades vs linnapiirkondades — marginaalide erinevused kuni 18%; (2) Sortimendi kättesaadavust — maapiirkondades oluliselt kitsendatum sortiment põhitoodetele; (3) Teenustingimuste erinevusi. Amet andis Coopile ettekirjutuse hindade ja teenustingimuste ühtlustamiseks maapiirkondades, tuginedes kohustusliku kauplemise doktriinile monopolistide suhtes.",
    outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["Konkurentsiseadus § 16", "§ 18"]), status: "final",
  },
  {
    case_number: "5-8/2022", title: "Eesti Raudtee — Raudteevõrgu juurdepääs kaubavedajatele",
    date: "2022-12-05", type: "sector_inquiry", sector: "transport",
    parties: JSON.stringify(["Eesti Raudtee AS", "Operail AS"]),
    summary: "Sektoriuuring raudtee infrastruktuuri juurdepääsutingimuste kohta. Amet hindas, kas Eesti Raudtee kohaldab diskrimineerivaid tingimusi era- ja riikliku kaubavedaja suhtes.",
    full_text: "Konkurentsiamet viis läbi sektoriuuringu raudteevõrgu juurdepääsu tingimuste osas. Eesti Raudtee AS opeerib raudteeinfrastruktuuri ja Operail AS teostab kaubavedusid. Uuringu tulemused: (1) Juurdepääsutasud on kooskõlas regulatiivsete nõuetega; (2) Sõiduplaani jaotamise protseduurid on läbipaistvad ja diskrimineerimisevabad; (3) Mõned probleemid avastati tehniliste standardite kohaldamises uustulnukatele. Amet andis soovitused juurdepääsuprotseduuri lihtsustamiseks, kuid ei tuvastanud Konkurentsiseaduse rikkumisi.",
    outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["Konkurentsiseadus § 10"]), status: "final",
  },
  {
    case_number: "5-2/2023", title: "Swedbank — Maksetariifide ühtsustamine",
    date: "2023-03-10", type: "abuse_of_dominance", sector: "financial_services",
    parties: JSON.stringify(["Swedbank AS"]),
    summary: "Uurimine Swedbank maksetariifide ühtsustamise praktika osas, kus amet hindas, kas panga dominantne seisund Eesti jaepanganduses võimaldab ebaõiglaste tingimuste kehtestamist.",
    full_text: "Konkurentsiamet analüüsis Swedbank AS-i maksetariifide praktikaid Eesti jaepanganduse turul. Swedbank hoiab Eestis umbes 40% turuosa jaepanganduses ja 35% maksetöötluses. Uurimise käigus hindas amet: (1) Maksekaartide vahendustasud — hindamaks kooskõla EL maksetariifide määrusega; (2) Arvelduskontode tasud — võrreldavate Balti turgudega; (3) Ettevõtjate makseaktsepteerimise tingimused. Amet leidis, et Swedbank tariifid vastavad üldiselt regulatiivsetele nõuetele ning ei tuvastanud Konkurentsiseaduse rikkumisi. Soovitused väikeettevõtjatele suunatud tariifide läbipaistvuse parandamiseks.",
    outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["Konkurentsiseadus § 16"]), status: "final",
  },
];

const insD = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insDAll = db.transaction(() => { for (const d of decisions) insD.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); });
insDAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Mergers ---
const mergers = [
  {
    case_number: "KO-1/2023", title: "Eesti Energia / Nelja Energia — Tuuleenergia ettevõtte omandamine",
    date: "2023-04-20", sector: "energy", acquiring_party: "Eesti Energia AS", target: "Nelja Energia AS",
    summary: "Konkurentsiamet kiitis heaks Eesti Energia poolt Nelja Energia omandamise. Amet leidis, et koondumine ei kahjusta oluliselt konkurentsi tuuleenergia turul, kuna Eesti Energia laieneb uuele tegevusvaldkonnale.",
    full_text: "Konkurentsiamet analüüsis Eesti Energia AS-i plaani omandada Nelja Energia AS — üks Eesti juhtivaid taastuvenergiaettevõtteid. Nelja Energia opereerib tuulepargid Eestis ja Lätis koguvõimsusega 250 MW. Hindamise käigus analüüsis amet: (1) Horisontaalsed mõjud — Eesti Energia tegutseb peamiselt traditsioonilises energeetilises ja pole varem tuuleenergiaturul aktiivne; (2) Vertikaalsed mõjud — Eesti Energia kontrollib jaotusinfrastruktuuri ja elektrimüüki; (3) Portfellimõju — kombineeritud energiaportfell ei loo turgu valitsevat seisundit. Amet kiitis koondumise heaks ilma tingimusteta, märkides et taastuvenergiaturul on palju konkurente ja koondumine toetab Eesti taastuvenergiapoliitikat.",
    outcome: "cleared_phase1", turnover: 1_200_000_000,
  },
  {
    case_number: "KO-3/2022", title: "Telia Eesti / Levira — Telepünktide võrgu omandamine",
    date: "2022-07-15", sector: "telecommunications", acquiring_party: "Telia Eesti AS", target: "Levira AS",
    summary: "Telia Eesti omandab Levira teleülekande- ja tornitaristu. Amet kiitis tehingu heaks tingimustega, nõudes juurdepääsu Levira tornidele konkureerivale infrastruktuuri- ettevõtetele.",
    full_text: "Konkurentsiamet hindas Telia Eesti AS-i plaani omandada Levira AS, mis opereerib televisiooni- ja raadio ülekandevõrku ning maanteede äärsete mastide infrastruktuuri. Levira tornid on oluline infrastruktuuri komponent mobiilsidevõrgu ja televisiooni edastamiseks. Menetluse käigus tuvastas amet järgmised mured: (1) Telia võib eelistada oma mobiilisidevõrgu vajadusi Levira tornide kasutamisel; (2) Konkureerivad teleoperaatorid sõltuvad Levira tornidele juurdepääsust; (3) Mastide asukohad on piiratud ressurss. Tingimused: Telia kohustus tagama mitteseotud turuosalistele juurdepääsu Levira tornidele turu keskmiste hindadega viie aasta jooksul pärast tehingut.",
    outcome: "cleared_with_conditions", turnover: 800_000_000,
  },
  {
    case_number: "KO-2/2023", title: "Maxima Eesti / Prisma — Kaubamajaketi omandamine",
    date: "2023-09-30", sector: "retail", acquiring_party: "Maxima Eesti OÜ", target: "Prisma Peremarket AS",
    summary: "Maxima Eesti omandab Prisma keti 9 kauplust. Amet kiitis koondumise heaks Tallinnas, kuid nõudis mõne piirkondliku kaupluse võõrandamist tiheasustusega piirkondades.",
    full_text: "Konkurentsiamet analüüsis Maxima Eesti OÜ soovi omandada Prisma Peremarket AS-i kõik üheksa kauplust Eestis. Maxima on Eestis kolmas suurim toidukaubandusvõrk umbes 18% turuosaga, Prisma omas umbes 7% turuosa. Koondumisega saavutaks Maxima 25% turuosa. Piirkondlik analüüs: (1) Tallinnas on mitmeid alternatiive (Rimi, Selver, Coop) — koondumine ei tekita monopoli; (2) Tartu linnakeskuses on Maximal ja Prismal kattuvad turupiirkonnad — tarbijatel piiratud valikuvõimalused pärast koondumist; (3) Mõnes väiksemas linnas olid Maxima ja Prisma ainsad suurkauplused. Tingimusena nõudis amet Tartu linnas 2 ja teises linnas 1 kaupluse võõrandamist kolmandale osapoolele.",
    outcome: "cleared_with_conditions", turnover: 650_000_000,
  },
];

const insM = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insMAll = db.transaction(() => { for (const m of mergers) insM.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); });
insMAll();
console.log(`Inserted ${mergers.length} mergers`);

const dCnt = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mCnt = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sCnt = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log(`\nSummary: ${sCnt} sectors, ${dCnt} decisions, ${mCnt} mergers`);
console.log(`Done. Database ready at ${DB_PATH}`);
db.close();
