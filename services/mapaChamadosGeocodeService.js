import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";

const TABELA_GEOCODE = "mapa_chamados_geocode";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ARQUIVO_GEOCODE = path.join(__dirname, "..", "data", "mapa_chamados_geocode.json");

let mariaDbPoolPromise = null;
let tabelaMariaDbGarantida = false;
let mariaDbAvisoEmitido = false;

function obterConfigMariaDb() {
    const host = process.env.AGENDA_TECNICOS_DB_HOST
        || process.env.MARIADB_HOST
        || process.env.MYSQL_HOST
        || process.env.DB_HOST;
    const database = process.env.AGENDA_TECNICOS_DB_NAME
        || process.env.MARIADB_DATABASE
        || process.env.MYSQL_DATABASE
        || process.env.DB_NAME
        || process.env.DATABASE_NAME;
    const user = process.env.AGENDA_TECNICOS_DB_USER
        || process.env.MARIADB_USER
        || process.env.MYSQL_USER
        || process.env.DB_USER;
    const password = process.env.AGENDA_TECNICOS_DB_PASSWORD
        || process.env.MARIADB_PASSWORD
        || process.env.MYSQL_PASSWORD
        || process.env.DB_PASSWORD
        || "";
    const port = Number(process.env.AGENDA_TECNICOS_DB_PORT
        || process.env.MARIADB_PORT
        || process.env.MYSQL_PORT
        || process.env.DB_PORT
        || 3306);

    if (!host || !database || !user) return null;

    return {
        host,
        database,
        user,
        password,
        port: Number.isInteger(port) && port > 0 ? port : 3306,
        connectionLimit: 5,
        timezone: "America/Sao_Paulo"
    };
}

function avisarMariaDbIndisponivel(err) {
    if (mariaDbAvisoEmitido) return;
    mariaDbAvisoEmitido = true;
    console.warn("⚠️ MariaDB indisponivel para cache de geocodificacao do mapa de chamados; usando store local.", err?.message || err);
}

async function obterPoolMariaDb() {
    const config = obterConfigMariaDb();
    if (!config) return null;

    if (!mariaDbPoolPromise) {
        mariaDbPoolPromise = import("mariadb")
            .then(modulo => {
                const mariaDb = modulo.default || modulo;
                return mariaDb.createPool(config);
            })
            .catch(err => {
                mariaDbPoolPromise = null;
                avisarMariaDbIndisponivel(err);
                return null;
            });
    }

    return mariaDbPoolPromise;
}

async function garantirTabelaMariaDb(conn) {
    if (tabelaMariaDbGarantida) return;

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELA_GEOCODE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            contrato_id VARCHAR(40) NOT NULL,
            cliente_id VARCHAR(40) NULL,
            address_hash CHAR(64) NOT NULL,
            latitude DECIMAL(10,7) NOT NULL,
            longitude DECIMAL(10,7) NOT NULL,
            provider VARCHAR(40) NOT NULL DEFAULT 'sgp',
            geocoded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uq_mapa_chamados_geocode_contrato (contrato_id),
            INDEX idx_mapa_chamados_geocode_hash (address_hash)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    tabelaMariaDbGarantida = true;
}

async function executarComMariaDb(callback) {
    const pool = await obterPoolMariaDb();
    if (!pool) return null;

    let conn;
    try {
        conn = await pool.getConnection();
        await garantirTabelaMariaDb(conn);
        const valor = await callback(conn);
        return { valor };
    } catch (err) {
        avisarMariaDbIndisponivel(err);
        return null;
    } finally {
        if (conn) conn.release();
    }
}

function garantirArquivoGeocode() {
    const dir = path.dirname(ARQUIVO_GEOCODE);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(ARQUIVO_GEOCODE)) {
        fs.writeFileSync(ARQUIVO_GEOCODE, JSON.stringify({ items: {} }, null, 2));
    }
}

function lerStoreGeocode() {
    garantirArquivoGeocode();
    const conteudo = fs.readFileSync(ARQUIVO_GEOCODE, "utf8");
    const dados = JSON.parse(conteudo || "{}");
    return { items: dados.items && typeof dados.items === "object" ? dados.items : {} };
}

function salvarStoreGeocode(store) {
    garantirArquivoGeocode();
    const temporario = `${ARQUIVO_GEOCODE}.tmp`;
    fs.writeFileSync(temporario, JSON.stringify(store, null, 2));
    fs.renameSync(temporario, ARQUIVO_GEOCODE);
}

export function calcularAddressHash(enderecoNormalizado) {
    return crypto.createHash("sha256").update(String(enderecoNormalizado || "").trim().toLowerCase()).digest("hex");
}

export async function obterGeocodeCache(contratoId) {
    if (!contratoId) return null;
    const chave = String(contratoId);

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT contrato_id, cliente_id, address_hash, latitude, longitude, provider, geocoded_at
            FROM ${TABELA_GEOCODE}
            WHERE contrato_id = ?
        `, [chave]);
        return rows[0] || null;
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreGeocode();
    return store.items[chave] || null;
}

export async function salvarGeocodeCache({ contratoId, clienteId = null, addressHash, latitude, longitude, provider = "nominatim" }) {
    if (!contratoId || !addressHash) return null;
    const chave = String(contratoId);
    const agora = new Date().toISOString();

    const resultadoDb = await executarComMariaDb(async (conn) => {
        await conn.query(`
            INSERT INTO ${TABELA_GEOCODE} (contrato_id, cliente_id, address_hash, latitude, longitude, provider)
            VALUES (?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                cliente_id = VALUES(cliente_id),
                address_hash = VALUES(address_hash),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                provider = VALUES(provider),
                geocoded_at = CURRENT_TIMESTAMP
        `, [chave, clienteId ? String(clienteId) : null, addressHash, latitude, longitude, provider]);

        const rows = await conn.query(`
            SELECT contrato_id, cliente_id, address_hash, latitude, longitude, provider, geocoded_at
            FROM ${TABELA_GEOCODE}
            WHERE contrato_id = ?
        `, [chave]);

        return rows[0] || null;
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreGeocode();
    const registro = {
        contrato_id: chave,
        cliente_id: clienteId ? String(clienteId) : null,
        address_hash: addressHash,
        latitude,
        longitude,
        provider,
        geocoded_at: agora
    };

    store.items[chave] = registro;
    salvarStoreGeocode(store);

    return registro;
}
