import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const MAPA_PONTOS_FIXOS_PERMISSION = "agenda_tecnicos_gerenciar_pontos_fixos";

const TABELA_PONTOS_FIXOS = "mapa_pontos_fixos";
const TIPOS_PONTOS_FIXOS = new Set([
    "pop",
    "casa_tecnico",
    "ponto_apoio",
    "area_critica",
    "observacao",
    "manutencao_recorrente",
    "outro"
]);
const VISIBILIDADES_PONTOS_FIXOS = new Set(["admin", "coordenador", "todos_autorizados"]);
const DEFAULTS_PONTOS_FIXOS = {
    pop: { icone: "🏢", cor: "#0f766e" },
    casa_tecnico: { icone: "🏠", cor: "#16a34a" },
    ponto_apoio: { icone: "📍", cor: "#2563eb" },
    area_critica: { icone: "⚠️", cor: "#dc2626" },
    observacao: { icone: "📝", cor: "#64748b" },
    manutencao_recorrente: { icone: "🛠️", cor: "#f97316" },
    outro: { icone: "•", cor: "#475569" }
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ARQUIVO_PONTOS_FIXOS = path.join(__dirname, "..", "data", "mapa_pontos_fixos.json");

let mariaDbPoolPromise = null;
let tabelaMariaDbGarantida = false;
let mariaDbAvisoEmitido = false;

export class MapaPontosFixosError extends Error {
    constructor(message, status = 400) {
        super(message);
        this.name = "MapaPontosFixosError";
        this.status = status;
    }
}

export function obterTiposPontosFixos() {
    return [...TIPOS_PONTOS_FIXOS];
}

export function obterVisibilidadesPontosFixos() {
    return [...VISIBILIDADES_PONTOS_FIXOS];
}

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
    console.warn("⚠️ MariaDB indisponivel para pontos fixos; usando store local persistente.", err?.message || err);
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
        CREATE TABLE IF NOT EXISTS ${TABELA_PONTOS_FIXOS} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(140) NOT NULL,
            tipo VARCHAR(40) NOT NULL,
            icone VARCHAR(16) NULL,
            cor VARCHAR(24) NULL,
            latitude DECIMAL(10,7) NOT NULL,
            longitude DECIMAL(10,7) NOT NULL,
            texto TEXT NULL,
            tecnico_id VARCHAR(80) NULL,
            visibilidade VARCHAR(32) NOT NULL DEFAULT 'coordenador',
            mostrar_label_no_mapa TINYINT(1) NOT NULL DEFAULT 0,
            ativo TINYINT(1) NOT NULL DEFAULT 1,
            criado_por VARCHAR(120) NULL,
            atualizado_por VARCHAR(120) NULL,
            removido_por VARCHAR(120) NULL,
            removido_em DATETIME NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_mapa_pontos_fixos_ativo_tipo (ativo, tipo),
            INDEX idx_mapa_pontos_fixos_visibilidade (visibilidade),
            INDEX idx_mapa_pontos_fixos_tecnico (tecnico_id)
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
        if (err instanceof MapaPontosFixosError) throw err;
        avisarMariaDbIndisponivel(err);
        return null;
    } finally {
        if (conn) conn.release();
    }
}

function criarStorePontosFixosVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function garantirArquivoPontosFixos() {
    const dir = path.dirname(ARQUIVO_PONTOS_FIXOS);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(ARQUIVO_PONTOS_FIXOS)) {
        fs.writeFileSync(ARQUIVO_PONTOS_FIXOS, JSON.stringify(criarStorePontosFixosVazio(), null, 2));
    }
}

function valorBooleano(valor, fallback = false) {
    if (valor === true || valor === 1 || valor === "1" || valor === "true" || valor === "sim") return true;
    if (valor === false || valor === 0 || valor === "0" || valor === "false" || valor === "nao") return false;
    return fallback;
}

function normalizarDataSaida(valor) {
    if (!valor) return null;
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) return valor.toISOString();
    return String(valor);
}

function coordenadaValida(latitudeOrigem, longitudeOrigem) {
    const latitude = Number(latitudeOrigem);
    const longitude = Number(longitudeOrigem);

    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
    if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
    if (latitude === 0 && longitude === 0) return null;

    return {
        latitude: Number(latitude.toFixed(7)),
        longitude: Number(longitude.toFixed(7))
    };
}

function serializarPontoFixo(registro = {}) {
    const coordenada = coordenadaValida(registro.latitude, registro.longitude);
    const tipo = TIPOS_PONTOS_FIXOS.has(String(registro.tipo || "")) ? String(registro.tipo) : "outro";
    const defaults = DEFAULTS_PONTOS_FIXOS[tipo] || DEFAULTS_PONTOS_FIXOS.outro;
    const mostrarLabel = valorBooleano(registro.mostrar_label_no_mapa ?? registro.mostrarLabelNoMapa, false);
    const ativo = valorBooleano(registro.ativo, true);

    return {
        id: Number(registro.id),
        nome: String(registro.nome || "").trim(),
        tipo,
        icone: String(registro.icone || defaults.icone).trim().slice(0, 8),
        cor: String(registro.cor || defaults.cor).trim().slice(0, 24),
        latitude: coordenada?.latitude ?? null,
        longitude: coordenada?.longitude ?? null,
        texto: String(registro.texto || "").trim(),
        tecnicoId: registro.tecnico_id || registro.tecnicoId || null,
        tecnico_id: registro.tecnico_id || registro.tecnicoId || null,
        visibilidade: VISIBILIDADES_PONTOS_FIXOS.has(String(registro.visibilidade || ""))
            ? String(registro.visibilidade)
            : "coordenador",
        mostrarLabelNoMapa: mostrarLabel,
        mostrar_label_no_mapa: mostrarLabel,
        ativo,
        criadoPor: registro.criado_por || registro.criadoPor || null,
        criado_por: registro.criado_por || registro.criadoPor || null,
        atualizadoPor: registro.atualizado_por || registro.atualizadoPor || null,
        atualizado_por: registro.atualizado_por || registro.atualizadoPor || null,
        removidoPor: registro.removido_por || registro.removidoPor || null,
        removido_por: registro.removido_por || registro.removidoPor || null,
        removidoEm: normalizarDataSaida(registro.removido_em || registro.removidoEm),
        removido_em: normalizarDataSaida(registro.removido_em || registro.removidoEm),
        createdAt: normalizarDataSaida(registro.created_at || registro.createdAt),
        created_at: normalizarDataSaida(registro.created_at || registro.createdAt),
        updatedAt: normalizarDataSaida(registro.updated_at || registro.updatedAt),
        updated_at: normalizarDataSaida(registro.updated_at || registro.updatedAt)
    };
}

function normalizarStorePontosFixos(store, tecnicosValidos = new Set()) {
    const agora = new Date().toISOString();
    const items = (Array.isArray(store?.items) ? store.items : [])
        .map((item, index) => serializarPontoFixo({
            ...item,
            id: Number(item.id) || index + 1,
            created_at: item.created_at || item.createdAt || agora,
            updated_at: item.updated_at || item.updatedAt || item.created_at || item.createdAt || agora
        }))
        .filter(item => item.id && item.nome && TIPOS_PONTOS_FIXOS.has(item.tipo))
        .filter(item => coordenadaValida(item.latitude, item.longitude))
        .map(item => ({
            ...item,
            tecnicoId: item.tecnicoId && (!tecnicosValidos.size || tecnicosValidos.has(String(item.tecnicoId)))
                ? String(item.tecnicoId)
                : null,
            tecnico_id: item.tecnicoId && (!tecnicosValidos.size || tecnicosValidos.has(String(item.tecnicoId)))
                ? String(item.tecnicoId)
                : null
        }));
    const maiorId = items.reduce((maior, item) => Math.max(maior, Number(item.id) || 0), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;

    return { nextId, items };
}

function lerStorePontosFixos(tecnicosValidos) {
    garantirArquivoPontosFixos();
    const conteudo = fs.readFileSync(ARQUIVO_PONTOS_FIXOS, "utf8");
    return normalizarStorePontosFixos(JSON.parse(conteudo || "{}"), tecnicosValidos);
}

function salvarStorePontosFixos(store, tecnicosValidos) {
    garantirArquivoPontosFixos();
    const normalizado = normalizarStorePontosFixos(store, tecnicosValidos);
    const temporario = `${ARQUIVO_PONTOS_FIXOS}.tmp`;

    fs.writeFileSync(temporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(temporario, ARQUIVO_PONTOS_FIXOS);

    return normalizado;
}

function normalizarCor(cor, tipo) {
    const valor = String(cor || "").trim();
    if (/^#[0-9a-f]{3}([0-9a-f]{3})?$/i.test(valor)) return valor;
    return DEFAULTS_PONTOS_FIXOS[tipo]?.cor || DEFAULTS_PONTOS_FIXOS.outro.cor;
}

function obterPayloadPontoFixo(payload = {}, atual = {}) {
    const tipo = String(payload.tipo || atual.tipo || "outro").trim();
    const tipoNormalizado = TIPOS_PONTOS_FIXOS.has(tipo) ? tipo : "";
    const defaults = DEFAULTS_PONTOS_FIXOS[tipoNormalizado] || DEFAULTS_PONTOS_FIXOS.outro;
    const coordenada = coordenadaValida(
        payload.latitude ?? atual.latitude,
        payload.longitude ?? atual.longitude
    );

    return {
        nome: String(payload.nome ?? atual.nome ?? "").trim().slice(0, 140),
        tipo: tipoNormalizado,
        icone: String(payload.icone ?? atual.icone ?? defaults.icone).trim().slice(0, 8) || defaults.icone,
        cor: normalizarCor(payload.cor ?? atual.cor ?? defaults.cor, tipoNormalizado || "outro"),
        latitude: coordenada?.latitude ?? null,
        longitude: coordenada?.longitude ?? null,
        texto: String(payload.texto ?? atual.texto ?? "").trim().slice(0, 1200),
        tecnicoId: String(payload.tecnicoId ?? payload.tecnico_id ?? atual.tecnico_id ?? atual.tecnicoId ?? "").trim() || null,
        visibilidade: VISIBILIDADES_PONTOS_FIXOS.has(String(payload.visibilidade ?? atual.visibilidade ?? ""))
            ? String(payload.visibilidade ?? atual.visibilidade)
            : "coordenador",
        mostrarLabelNoMapa: valorBooleano(
            payload.mostrarLabelNoMapa ?? payload.mostrar_label_no_mapa ?? atual.mostrar_label_no_mapa ?? atual.mostrarLabelNoMapa,
            false
        )
    };
}

function validarPayloadPontoFixo(payload, tecnicosValidos) {
    if (!payload.nome) {
        throw new MapaPontosFixosError("Informe o nome do ponto fixo.", 400);
    }

    if (!TIPOS_PONTOS_FIXOS.has(payload.tipo)) {
        throw new MapaPontosFixosError("Tipo de ponto fixo invalido.", 400);
    }

    if (payload.tipo === "central") {
        throw new MapaPontosFixosError("Use o tipo POP / Base; nao existe tipo separado de central.", 400);
    }

    if (!coordenadaValida(payload.latitude, payload.longitude)) {
        throw new MapaPontosFixosError("Informe latitude e longitude validas.", 400);
    }

    if (!VISIBILIDADES_PONTOS_FIXOS.has(payload.visibilidade)) {
        throw new MapaPontosFixosError("Visibilidade invalida.", 400);
    }

    if (payload.tecnicoId && tecnicosValidos?.size && !tecnicosValidos.has(payload.tecnicoId)) {
        throw new MapaPontosFixosError("Tecnico vinculado nao encontrado.", 404);
    }
}

function pontoVisivelParaPerfil(ponto, perfil = {}) {
    if (!ponto.ativo) return false;
    if (perfil.admin || perfil.semAutenticacaoLocal) return true;
    if (perfil.coordenador) return ponto.visibilidade !== "admin";
    return ponto.visibilidade === "todos_autorizados" && ponto.tipo !== "casa_tecnico";
}

function filtrarPontosPorPerfil(pontos, perfil) {
    return pontos.filter(ponto => pontoVisivelParaPerfil(ponto, perfil));
}

async function listarPontosFixosMariaDb(conn, perfil) {
    const rows = await conn.query(`
        SELECT id, nome, tipo, icone, cor, latitude, longitude, texto, tecnico_id, visibilidade,
               mostrar_label_no_mapa, ativo, criado_por, atualizado_por, removido_por, removido_em,
               created_at, updated_at
        FROM ${TABELA_PONTOS_FIXOS}
        WHERE ativo = 1
        ORDER BY tipo ASC, nome ASC
    `);

    return filtrarPontosPorPerfil(rows.map(serializarPontoFixo), perfil);
}

function listarPontosFixosLocal(perfil, tecnicosValidos) {
    return filtrarPontosPorPerfil(lerStorePontosFixos(tecnicosValidos).items, perfil)
        .sort((a, b) => a.tipo.localeCompare(b.tipo, "pt-BR") || a.nome.localeCompare(b.nome, "pt-BR"));
}

export async function listarPontosFixosMapa(perfil = {}, tecnicosValidos = new Set()) {
    const resultadoDb = await executarComMariaDb(conn => listarPontosFixosMariaDb(conn, perfil));
    if (resultadoDb) return resultadoDb.valor;

    return listarPontosFixosLocal(perfil, tecnicosValidos);
}

export async function criarPontoFixoMapa(payloadOrigem, { tecnicosValidos = new Set(), criadoPor = null } = {}) {
    const payload = obterPayloadPontoFixo(payloadOrigem);
    validarPayloadPontoFixo(payload, tecnicosValidos);

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const resultado = await conn.query(`
            INSERT INTO ${TABELA_PONTOS_FIXOS}
                (nome, tipo, icone, cor, latitude, longitude, texto, tecnico_id, visibilidade,
                 mostrar_label_no_mapa, ativo, criado_por, atualizado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        `, [
            payload.nome,
            payload.tipo,
            payload.icone,
            payload.cor,
            payload.latitude,
            payload.longitude,
            payload.texto || null,
            payload.tecnicoId || null,
            payload.visibilidade,
            payload.mostrarLabelNoMapa ? 1 : 0,
            criadoPor || null,
            criadoPor || null
        ]);
        const rows = await conn.query(`
            SELECT id, nome, tipo, icone, cor, latitude, longitude, texto, tecnico_id, visibilidade,
                   mostrar_label_no_mapa, ativo, criado_por, atualizado_por, removido_por, removido_em,
                   created_at, updated_at
            FROM ${TABELA_PONTOS_FIXOS}
            WHERE id = ?
        `, [Number(resultado.insertId)]);

        return serializarPontoFixo(rows[0]);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStorePontosFixos(tecnicosValidos);
    const agora = new Date().toISOString();
    const item = {
        id: store.nextId,
        nome: payload.nome,
        tipo: payload.tipo,
        icone: payload.icone,
        cor: payload.cor,
        latitude: payload.latitude,
        longitude: payload.longitude,
        texto: payload.texto,
        tecnico_id: payload.tecnicoId,
        visibilidade: payload.visibilidade,
        mostrar_label_no_mapa: payload.mostrarLabelNoMapa,
        ativo: true,
        criado_por: criadoPor || null,
        atualizado_por: criadoPor || null,
        removido_por: null,
        removido_em: null,
        created_at: agora,
        updated_at: agora
    };

    store.nextId += 1;
    store.items.push(item);
    salvarStorePontosFixos(store, tecnicosValidos);

    return serializarPontoFixo(item);
}

export async function atualizarPontoFixoMapa(idOrigem, payloadOrigem, { tecnicosValidos = new Set(), atualizadoPor = null } = {}) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new MapaPontosFixosError("Ponto fixo invalido.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, nome, tipo, icone, cor, latitude, longitude, texto, tecnico_id, visibilidade,
                   mostrar_label_no_mapa, ativo, criado_por, atualizado_por, removido_por, removido_em,
                   created_at, updated_at
            FROM ${TABELA_PONTOS_FIXOS}
            WHERE id = ? AND ativo = 1
        `, [id]);
        const atual = rows[0];
        if (!atual) throw new MapaPontosFixosError("Ponto fixo nao encontrado.", 404);

        const payload = obterPayloadPontoFixo(payloadOrigem, atual);
        validarPayloadPontoFixo(payload, tecnicosValidos);

        await conn.query(`
            UPDATE ${TABELA_PONTOS_FIXOS}
            SET nome = ?, tipo = ?, icone = ?, cor = ?, latitude = ?, longitude = ?, texto = ?,
                tecnico_id = ?, visibilidade = ?, mostrar_label_no_mapa = ?, atualizado_por = ?
            WHERE id = ? AND ativo = 1
        `, [
            payload.nome,
            payload.tipo,
            payload.icone,
            payload.cor,
            payload.latitude,
            payload.longitude,
            payload.texto || null,
            payload.tecnicoId || null,
            payload.visibilidade,
            payload.mostrarLabelNoMapa ? 1 : 0,
            atualizadoPor || null,
            id
        ]);

        const atualizados = await conn.query(`
            SELECT id, nome, tipo, icone, cor, latitude, longitude, texto, tecnico_id, visibilidade,
                   mostrar_label_no_mapa, ativo, criado_por, atualizado_por, removido_por, removido_em,
                   created_at, updated_at
            FROM ${TABELA_PONTOS_FIXOS}
            WHERE id = ?
        `, [id]);

        return serializarPontoFixo(atualizados[0]);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStorePontosFixos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id && registro.ativo !== false);
    if (!item) throw new MapaPontosFixosError("Ponto fixo nao encontrado.", 404);

    const payload = obterPayloadPontoFixo(payloadOrigem, item);
    validarPayloadPontoFixo(payload, tecnicosValidos);

    item.nome = payload.nome;
    item.tipo = payload.tipo;
    item.icone = payload.icone;
    item.cor = payload.cor;
    item.latitude = payload.latitude;
    item.longitude = payload.longitude;
    item.texto = payload.texto;
    item.tecnico_id = payload.tecnicoId;
    item.tecnicoId = payload.tecnicoId;
    item.visibilidade = payload.visibilidade;
    item.mostrar_label_no_mapa = payload.mostrarLabelNoMapa;
    item.mostrarLabelNoMapa = payload.mostrarLabelNoMapa;
    item.atualizado_por = atualizadoPor || null;
    item.atualizadoPor = atualizadoPor || null;
    item.updated_at = new Date().toISOString();
    item.updatedAt = item.updated_at;

    salvarStorePontosFixos(store, tecnicosValidos);

    return serializarPontoFixo(item);
}

export async function removerPontoFixoMapa(idOrigem, { tecnicosValidos = new Set(), removidoPor = null } = {}) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new MapaPontosFixosError("Ponto fixo invalido.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, nome, tipo, icone, cor, latitude, longitude, texto, tecnico_id, visibilidade,
                   mostrar_label_no_mapa, ativo, criado_por, atualizado_por, removido_por, removido_em,
                   created_at, updated_at
            FROM ${TABELA_PONTOS_FIXOS}
            WHERE id = ? AND ativo = 1
        `, [id]);
        const atual = rows[0];
        if (!atual) throw new MapaPontosFixosError("Ponto fixo nao encontrado.", 404);

        await conn.query(`
            UPDATE ${TABELA_PONTOS_FIXOS}
            SET ativo = 0, removido_por = ?, removido_em = NOW(), atualizado_por = ?
            WHERE id = ?
        `, [removidoPor || null, removidoPor || null, id]);

        return serializarPontoFixo({
            ...atual,
            ativo: 0,
            removido_por: removidoPor || null,
            removido_em: new Date().toISOString()
        });
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStorePontosFixos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id && registro.ativo !== false);
    if (!item) throw new MapaPontosFixosError("Ponto fixo nao encontrado.", 404);

    item.ativo = false;
    item.removido_por = removidoPor || null;
    item.removidoPor = removidoPor || null;
    item.removido_em = new Date().toISOString();
    item.removidoEm = item.removido_em;
    item.atualizado_por = removidoPor || null;
    item.atualizadoPor = removidoPor || null;
    item.updated_at = item.removido_em;
    item.updatedAt = item.updated_at;

    salvarStorePontosFixos(store, tecnicosValidos);

    return serializarPontoFixo(item);
}
