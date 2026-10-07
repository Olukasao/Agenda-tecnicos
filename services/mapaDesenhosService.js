import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const MAPA_DESENHOS_PERMISSION = "agenda_tecnicos_gerenciar_desenhos";

const TABELA_DESENHOS = "mapa_desenhos";
const TIPOS_DESENHO = new Set(["temporario_dia", "permanente"]);
const FERRAMENTAS_DESENHO = new Set([
    "caneta",
    "marca_texto",
    "linha",
    "seta",
    "retangulo",
    "circulo",
    "poligono",
    "area_regiao",
    "texto",
    "marcador"
]);
const TIPOS_AREA_DESENHO = new Set([
    "regiao_trabalho",
    "area_prioridade",
    "area_critica",
    "area_evitar",
    "setor_instalacao",
    "setor_manutencao",
    "observacao_area",
    "outro"
]);
const VISIBILIDADES_DESENHO = new Set(["admin", "coordenador", "todos_autorizados"]);
const DEFAULTS_DESENHO = {
    caneta: { cor: "#2563eb", preenchimentoCor: "#2563eb", espessura: 4 },
    marca_texto: { cor: "#facc15", preenchimentoCor: "#facc15", espessura: 14 },
    linha: { cor: "#2563eb", preenchimentoCor: "#2563eb", espessura: 3 },
    seta: { cor: "#f97316", preenchimentoCor: "#f97316", espessura: 3 },
    retangulo: { cor: "#0f766e", preenchimentoCor: "#0f766e", espessura: 3 },
    circulo: { cor: "#7c3aed", preenchimentoCor: "#7c3aed", espessura: 3 },
    poligono: { cor: "#22c55e", preenchimentoCor: "#22c55e", espessura: 3 },
    area_regiao: { cor: "#22c55e", preenchimentoCor: "#22c55e", espessura: 3 },
    texto: { cor: "#111827", preenchimentoCor: "#111827", espessura: 2 },
    marcador: { cor: "#ef4444", preenchimentoCor: "#ef4444", espessura: 2 }
};

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ARQUIVO_DESENHOS = path.join(__dirname, "..", "data", "mapa_desenhos.json");

let mariaDbPoolPromise = null;
let tabelaMariaDbGarantida = false;
let mariaDbAvisoEmitido = false;

export class MapaDesenhosError extends Error {
    constructor(message, status = 400) {
        super(message);
        this.name = "MapaDesenhosError";
        this.status = status;
    }
}

export function obterFerramentasDesenhoMapa() {
    return [...FERRAMENTAS_DESENHO];
}

export function obterTiposAreaDesenhoMapa() {
    return [...TIPOS_AREA_DESENHO];
}

export function obterVisibilidadesDesenhoMapa() {
    return [...VISIBILIDADES_DESENHO];
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
    console.warn("⚠️ MariaDB indisponivel para desenhos do mapa; usando store local persistente.", err?.message || err);
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
        CREATE TABLE IF NOT EXISTS ${TABELA_DESENHOS} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            titulo VARCHAR(160) NOT NULL,
            tipo VARCHAR(32) NOT NULL DEFAULT 'temporario_dia',
            ferramenta VARCHAR(40) NOT NULL,
            area_tipo VARCHAR(60) NULL,
            geometria_json JSON NOT NULL,
            texto TEXT NULL,
            cor VARCHAR(24) NULL,
            espessura DECIMAL(8,2) NULL,
            opacidade DECIMAL(4,3) NULL,
            preenchimento_cor VARCHAR(24) NULL,
            preenchimento_opacidade DECIMAL(4,3) NULL,
            cor_borda VARCHAR(24) NULL,
            opacidade_borda DECIMAL(4,3) NULL,
            tecnicos_vinculados_json JSON NULL,
            equipe_vinculada VARCHAR(120) NULL,
            data_referencia DATE NULL,
            permanente TINYINT(1) NOT NULL DEFAULT 0,
            visibilidade VARCHAR(32) NOT NULL DEFAULT 'coordenador',
            criado_por VARCHAR(120) NULL,
            atualizado_por VARCHAR(120) NULL,
            removido_por VARCHAR(120) NULL,
            removido_em DATETIME NULL,
            ativo TINYINT(1) NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_mapa_desenhos_data (ativo, data_referencia, permanente),
            INDEX idx_mapa_desenhos_ferramenta (ativo, ferramenta),
            INDEX idx_mapa_desenhos_visibilidade (visibilidade)
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
        if (err instanceof MapaDesenhosError) throw err;
        avisarMariaDbIndisponivel(err);
        return null;
    } finally {
        if (conn) conn.release();
    }
}

function criarStoreDesenhosVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function garantirArquivoDesenhos() {
    const dir = path.dirname(ARQUIVO_DESENHOS);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(ARQUIVO_DESENHOS)) {
        fs.writeFileSync(ARQUIVO_DESENHOS, JSON.stringify(criarStoreDesenhosVazio(), null, 2));
    }
}

function valorBooleano(valor, fallback = false) {
    if (valor === true || valor === 1 || valor === "1" || valor === "true" || valor === "sim") return true;
    if (valor === false || valor === 0 || valor === "0" || valor === "false" || valor === "nao") return false;
    return fallback;
}

function parseJsonSeguro(valor, fallback) {
    if (valor === null || valor === undefined || valor === "") return fallback;
    if (typeof valor === "object") return valor;

    try {
        return JSON.parse(String(valor));
    } catch {
        return fallback;
    }
}

function normalizarDataReferencia(valor) {
    if (!valor) return null;
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) return valor.toISOString().slice(0, 10);

    const texto = String(valor).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(texto)) return texto;

    const data = new Date(texto);
    if (!Number.isNaN(data.getTime())) return data.toISOString().slice(0, 10);

    return null;
}

function normalizarDataSaida(valor) {
    if (!valor) return null;
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) return valor.toISOString();
    return String(valor);
}

function coordenadaValida(ponto = {}) {
    const latitude = Number(ponto.lat ?? ponto.latitude);
    const longitude = Number(ponto.lng ?? ponto.longitude);

    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
    if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
    if (latitude === 0 && longitude === 0) return null;

    return {
        lat: Number(latitude.toFixed(7)),
        lng: Number(longitude.toFixed(7))
    };
}

function normalizarPontoGeometria(ponto) {
    const coordenada = coordenadaValida(ponto);
    if (!coordenada) return null;
    return coordenada;
}

function normalizarGeometria(geometriaOrigem, ferramenta) {
    const geometria = parseJsonSeguro(geometriaOrigem, {});
    const tipoPadrao = ["area_regiao", "poligono"].includes(ferramenta)
        ? "polygon"
        : (["linha", "seta", "caneta", "marca_texto"].includes(ferramenta) ? "polyline" : ferramenta);
    const points = Array.isArray(geometria.points)
        ? geometria.points.map(normalizarPontoGeometria).filter(Boolean)
        : [];
    const point = normalizarPontoGeometria(geometria.point);
    const center = normalizarPontoGeometria(geometria.center);
    const labelPosition = normalizarPontoGeometria(geometria.labelPosition);
    const radius = Number(geometria.radius || geometria.raio || 0);
    const resultado = {
        type: String(geometria.type || tipoPadrao),
        points
    };

    if (point) resultado.point = point;
    if (center) resultado.center = center;
    if (labelPosition) resultado.labelPosition = labelPosition;
    if (Number.isFinite(radius) && radius > 0) resultado.radius = Number(radius.toFixed(2));

    return resultado;
}

function obterPontosValidacaoGeometria(geometria) {
    return [
        ...(Array.isArray(geometria.points) ? geometria.points : []),
        geometria.point,
        geometria.center,
        geometria.labelPosition
    ].filter(Boolean);
}

function normalizarCor(cor, fallback = "#2563eb") {
    const valor = String(cor || "").trim();
    if (/^#[0-9a-f]{3}([0-9a-f]{3})?$/i.test(valor)) return valor;
    return fallback;
}

function normalizarNumero(valor, fallback, min, max) {
    const numero = Number(valor);
    if (!Number.isFinite(numero)) return fallback;
    return Math.min(max, Math.max(min, numero));
}

function normalizarTecnicosVinculados(valor, tecnicosValidos = new Set()) {
    const lista = Array.isArray(valor)
        ? valor
        : parseJsonSeguro(valor, []);

    if (!Array.isArray(lista)) return [];

    return [...new Set(lista
        .map(item => String(item || "").trim())
        .filter(Boolean)
        .filter(item => !tecnicosValidos.size || tecnicosValidos.has(item)))]
        .slice(0, 30);
}

function normalizarStoreDesenhos(store, tecnicosValidos = new Set()) {
    const agora = new Date().toISOString();
    const items = (Array.isArray(store?.items) ? store.items : [])
        .map((item, index) => serializarDesenhoMapa({
            ...item,
            id: Number(item.id) || index + 1,
            created_at: item.created_at || item.createdAt || agora,
            updated_at: item.updated_at || item.updatedAt || item.created_at || item.createdAt || agora
        }, tecnicosValidos))
        .filter(item => item.id && item.titulo && FERRAMENTAS_DESENHO.has(item.ferramenta));
    const maiorId = items.reduce((maior, item) => Math.max(maior, Number(item.id) || 0), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;

    return { nextId, items };
}

function lerStoreDesenhos(tecnicosValidos) {
    garantirArquivoDesenhos();
    const conteudo = fs.readFileSync(ARQUIVO_DESENHOS, "utf8");
    return normalizarStoreDesenhos(JSON.parse(conteudo || "{}"), tecnicosValidos);
}

function salvarStoreDesenhos(store, tecnicosValidos) {
    garantirArquivoDesenhos();
    const normalizado = normalizarStoreDesenhos(store, tecnicosValidos);
    const temporario = `${ARQUIVO_DESENHOS}.tmp`;

    fs.writeFileSync(temporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(temporario, ARQUIVO_DESENHOS);

    return normalizado;
}

function obterPayloadDesenho(payload = {}, atual = {}, { dataReferenciaPadrao = null, tecnicosValidos = new Set() } = {}) {
    const ferramenta = String(payload.ferramenta ?? atual.ferramenta ?? "area_regiao").trim();
    const ferramentaNormalizada = FERRAMENTAS_DESENHO.has(ferramenta) ? ferramenta : "";
    const defaults = DEFAULTS_DESENHO[ferramentaNormalizada] || DEFAULTS_DESENHO.area_regiao;
    const permanente = valorBooleano(payload.permanente ?? atual.permanente, false)
        || String(payload.tipo ?? atual.tipo ?? "") === "permanente";
    const tipo = permanente ? "permanente" : "temporario_dia";
    const areaTipoOrigem = payload.areaTipo ?? payload.area_tipo ?? payload.tipoArea ?? payload.tipo_area ?? atual.area_tipo ?? atual.areaTipo;
    const areaTipo = TIPOS_AREA_DESENHO.has(String(areaTipoOrigem || "")) ? String(areaTipoOrigem) : "outro";
    const cor = normalizarCor(payload.cor ?? atual.cor, defaults.cor);
    const corBorda = normalizarCor(payload.corBorda ?? payload.cor_borda ?? atual.cor_borda ?? atual.corBorda ?? cor, cor);
    const preenchimentoCor = normalizarCor(
        payload.preenchimentoCor ?? payload.preenchimento_cor ?? payload.corPreenchimento ?? atual.preenchimento_cor ?? atual.preenchimentoCor ?? defaults.preenchimentoCor,
        defaults.preenchimentoCor
    );
    const geometria = normalizarGeometria(payload.geometriaJson ?? payload.geometria_json ?? atual.geometria_json ?? atual.geometriaJson, ferramentaNormalizada);
    const dataReferencia = permanente
        ? null
        : normalizarDataReferencia(payload.dataReferencia ?? payload.data_referencia ?? atual.data_referencia ?? atual.dataReferencia ?? dataReferenciaPadrao);

    return {
        titulo: String(payload.titulo ?? payload.nome ?? atual.titulo ?? atual.nome ?? "").trim().slice(0, 160),
        tipo,
        ferramenta: ferramentaNormalizada,
        areaTipo,
        geometriaJson: geometria,
        texto: String(payload.texto ?? payload.descricao ?? atual.texto ?? "").trim().slice(0, 2000),
        cor,
        espessura: normalizarNumero(payload.espessura ?? atual.espessura, defaults.espessura, 1, 18),
        opacidade: normalizarNumero(payload.opacidade ?? atual.opacidade, 0.9, 0.05, 1),
        preenchimentoCor,
        preenchimentoOpacidade: normalizarNumero(
            payload.preenchimentoOpacidade ?? payload.preenchimento_opacidade ?? atual.preenchimento_opacidade ?? atual.preenchimentoOpacidade,
            0.25,
            0,
            0.8
        ),
        corBorda,
        opacidadeBorda: normalizarNumero(
            payload.opacidadeBorda ?? payload.opacidade_borda ?? atual.opacidade_borda ?? atual.opacidadeBorda,
            0.8,
            0.05,
            1
        ),
        tecnicosVinculados: normalizarTecnicosVinculados(
            payload.tecnicosVinculados ?? payload.tecnicos_vinculados_json ?? atual.tecnicos_vinculados_json ?? atual.tecnicosVinculados,
            tecnicosValidos
        ),
        equipeVinculada: String(payload.equipeVinculada ?? payload.equipe_vinculada ?? atual.equipe_vinculada ?? atual.equipeVinculada ?? "").trim().slice(0, 120),
        dataReferencia,
        permanente,
        visibilidade: VISIBILIDADES_DESENHO.has(String(payload.visibilidade ?? atual.visibilidade ?? ""))
            ? String(payload.visibilidade ?? atual.visibilidade)
            : "coordenador"
    };
}

function validarPayloadDesenho(payload) {
    if (!payload.titulo) {
        throw new MapaDesenhosError("Informe o nome da marcação.", 400);
    }

    if (!TIPOS_DESENHO.has(payload.tipo)) {
        throw new MapaDesenhosError("Tipo de marcação invalido.", 400);
    }

    if (!FERRAMENTAS_DESENHO.has(payload.ferramenta)) {
        throw new MapaDesenhosError("Ferramenta de desenho invalida.", 400);
    }

    if (!payload.permanente && !payload.dataReferencia) {
        throw new MapaDesenhosError("Informe a data de referencia da marcação temporária.", 400);
    }

    if (!VISIBILIDADES_DESENHO.has(payload.visibilidade)) {
        throw new MapaDesenhosError("Visibilidade invalida.", 400);
    }

    const pontos = obterPontosValidacaoGeometria(payload.geometriaJson);
    if (!pontos.length) {
        throw new MapaDesenhosError("Informe uma geometria valida para a marcação.", 400);
    }

    if (["area_regiao", "poligono"].includes(payload.ferramenta) && payload.geometriaJson.points.length < 3) {
        throw new MapaDesenhosError("A área precisa de pelo menos 3 pontos.", 400);
    }

    if (["linha", "seta", "caneta", "marca_texto"].includes(payload.ferramenta) && payload.geometriaJson.points.length < 2) {
        throw new MapaDesenhosError("A marcação precisa de pelo menos 2 pontos.", 400);
    }

    if (payload.ferramenta === "retangulo" && payload.geometriaJson.points.length < 2) {
        throw new MapaDesenhosError("O retângulo precisa de 2 cantos.", 400);
    }

    if (payload.ferramenta === "circulo" && (!payload.geometriaJson.center || !payload.geometriaJson.radius)) {
        throw new MapaDesenhosError("O círculo precisa de centro e raio.", 400);
    }

    if (["texto", "marcador"].includes(payload.ferramenta) && !payload.geometriaJson.point) {
        throw new MapaDesenhosError("Informe o ponto da marcação no mapa.", 400);
    }
}

function serializarDesenhoMapa(registro = {}, tecnicosValidos = new Set()) {
    const ferramenta = FERRAMENTAS_DESENHO.has(String(registro.ferramenta || "")) ? String(registro.ferramenta) : "area_regiao";
    const permanente = valorBooleano(registro.permanente, String(registro.tipo || "") === "permanente");
    const tipo = permanente ? "permanente" : (TIPOS_DESENHO.has(String(registro.tipo || "")) ? String(registro.tipo) : "temporario_dia");
    const areaTipo = TIPOS_AREA_DESENHO.has(String(registro.area_tipo || registro.areaTipo || ""))
        ? String(registro.area_tipo || registro.areaTipo)
        : "outro";
    const defaults = DEFAULTS_DESENHO[ferramenta] || DEFAULTS_DESENHO.area_regiao;
    const geometriaJson = normalizarGeometria(registro.geometria_json ?? registro.geometriaJson, ferramenta);
    const tecnicosVinculados = normalizarTecnicosVinculados(registro.tecnicos_vinculados_json ?? registro.tecnicosVinculados, tecnicosValidos);
    const dataReferencia = normalizarDataReferencia(registro.data_referencia || registro.dataReferencia);
    const ativo = valorBooleano(registro.ativo, true);

    return {
        id: Number(registro.id),
        titulo: String(registro.titulo || registro.nome || "").trim(),
        nome: String(registro.titulo || registro.nome || "").trim(),
        tipo,
        ferramenta,
        areaTipo,
        area_tipo: areaTipo,
        geometriaJson,
        geometria_json: geometriaJson,
        texto: String(registro.texto || "").trim(),
        descricao: String(registro.texto || "").trim(),
        cor: normalizarCor(registro.cor, defaults.cor),
        espessura: normalizarNumero(registro.espessura, defaults.espessura, 1, 18),
        opacidade: normalizarNumero(registro.opacidade, 0.9, 0.05, 1),
        preenchimentoCor: normalizarCor(registro.preenchimento_cor ?? registro.preenchimentoCor, defaults.preenchimentoCor),
        preenchimento_cor: normalizarCor(registro.preenchimento_cor ?? registro.preenchimentoCor, defaults.preenchimentoCor),
        preenchimentoOpacidade: normalizarNumero(registro.preenchimento_opacidade ?? registro.preenchimentoOpacidade, 0.25, 0, 0.8),
        preenchimento_opacidade: normalizarNumero(registro.preenchimento_opacidade ?? registro.preenchimentoOpacidade, 0.25, 0, 0.8),
        corBorda: normalizarCor(registro.cor_borda ?? registro.corBorda ?? registro.cor, defaults.cor),
        cor_borda: normalizarCor(registro.cor_borda ?? registro.corBorda ?? registro.cor, defaults.cor),
        opacidadeBorda: normalizarNumero(registro.opacidade_borda ?? registro.opacidadeBorda, 0.8, 0.05, 1),
        opacidade_borda: normalizarNumero(registro.opacidade_borda ?? registro.opacidadeBorda, 0.8, 0.05, 1),
        tecnicosVinculados,
        tecnicos_vinculados_json: tecnicosVinculados,
        equipeVinculada: String(registro.equipe_vinculada || registro.equipeVinculada || "").trim(),
        equipe_vinculada: String(registro.equipe_vinculada || registro.equipeVinculada || "").trim(),
        dataReferencia,
        data_referencia: dataReferencia,
        permanente,
        visibilidade: VISIBILIDADES_DESENHO.has(String(registro.visibilidade || "")) ? String(registro.visibilidade) : "coordenador",
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

function desenhoVisivelParaPerfil(desenho, perfil = {}) {
    if (!desenho.ativo) return false;
    if (perfil.admin || perfil.semAutenticacaoLocal) return true;
    if (perfil.coordenador) return desenho.visibilidade !== "admin";
    return desenho.visibilidade === "todos_autorizados";
}

function filtrarDesenhosPorPerfil(desenhos, perfil) {
    return desenhos.filter(desenho => desenhoVisivelParaPerfil(desenho, perfil));
}

async function listarDesenhosMariaDb(conn, { dataReferencia, perfil, tecnicosValidos }) {
    const rows = await conn.query(`
        SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
               preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
               tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
               criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
        FROM ${TABELA_DESENHOS}
        WHERE ativo = 1
          AND (permanente = 1 OR tipo = 'permanente' OR data_referencia = ?)
        ORDER BY permanente ASC, ferramenta ASC, updated_at DESC
    `, [dataReferencia]);

    return filtrarDesenhosPorPerfil(rows.map(row => serializarDesenhoMapa(row, tecnicosValidos)), perfil);
}

function listarDesenhosLocal({ dataReferencia, perfil, tecnicosValidos }) {
    return filtrarDesenhosPorPerfil(lerStoreDesenhos(tecnicosValidos).items, perfil)
        .filter(desenho => desenho.permanente || desenho.tipo === "permanente" || desenho.dataReferencia === dataReferencia)
        .sort((a, b) => {
            if (a.permanente !== b.permanente) return Number(a.permanente) - Number(b.permanente);
            return String(b.updatedAt || "").localeCompare(String(a.updatedAt || ""));
        });
}

export async function listarDesenhosMapa({ dataReferencia, perfil = {}, tecnicosValidos = new Set() } = {}) {
    const dataNormalizada = normalizarDataReferencia(dataReferencia);
    if (!dataNormalizada) throw new MapaDesenhosError("Informe uma data valida para listar desenhos.", 400);

    const resultadoDb = await executarComMariaDb(conn => listarDesenhosMariaDb(conn, {
        dataReferencia: dataNormalizada,
        perfil,
        tecnicosValidos
    }));
    if (resultadoDb) return resultadoDb.valor;

    return listarDesenhosLocal({ dataReferencia: dataNormalizada, perfil, tecnicosValidos });
}

export async function criarDesenhoMapa(payloadOrigem, { tecnicosValidos = new Set(), dataReferencia = null, criadoPor = null } = {}) {
    const payload = obterPayloadDesenho(payloadOrigem, {}, {
        dataReferenciaPadrao: dataReferencia,
        tecnicosValidos
    });
    validarPayloadDesenho(payload);

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const resultado = await conn.query(`
            INSERT INTO ${TABELA_DESENHOS}
                (titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                 preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                 tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                 ativo, criado_por, atualizado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
        `, [
            payload.titulo,
            payload.tipo,
            payload.ferramenta,
            payload.areaTipo,
            JSON.stringify(payload.geometriaJson),
            payload.texto || null,
            payload.cor,
            payload.espessura,
            payload.opacidade,
            payload.preenchimentoCor,
            payload.preenchimentoOpacidade,
            payload.corBorda,
            payload.opacidadeBorda,
            JSON.stringify(payload.tecnicosVinculados),
            payload.equipeVinculada || null,
            payload.dataReferencia || null,
            payload.permanente ? 1 : 0,
            payload.visibilidade,
            criadoPor || null,
            criadoPor || null
        ]);

        const rows = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE id = ?
        `, [Number(resultado.insertId)]);

        return serializarDesenhoMapa(rows[0], tecnicosValidos);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreDesenhos(tecnicosValidos);
    const agora = new Date().toISOString();
    const item = {
        id: store.nextId,
        titulo: payload.titulo,
        tipo: payload.tipo,
        ferramenta: payload.ferramenta,
        area_tipo: payload.areaTipo,
        geometria_json: payload.geometriaJson,
        texto: payload.texto,
        cor: payload.cor,
        espessura: payload.espessura,
        opacidade: payload.opacidade,
        preenchimento_cor: payload.preenchimentoCor,
        preenchimento_opacidade: payload.preenchimentoOpacidade,
        cor_borda: payload.corBorda,
        opacidade_borda: payload.opacidadeBorda,
        tecnicos_vinculados_json: payload.tecnicosVinculados,
        equipe_vinculada: payload.equipeVinculada,
        data_referencia: payload.dataReferencia,
        permanente: payload.permanente,
        visibilidade: payload.visibilidade,
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
    salvarStoreDesenhos(store, tecnicosValidos);

    return serializarDesenhoMapa(item, tecnicosValidos);
}

export async function atualizarDesenhoMapa(idOrigem, payloadOrigem, { tecnicosValidos = new Set(), dataReferencia = null, atualizadoPor = null } = {}) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new MapaDesenhosError("Marcação invalida.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE id = ? AND ativo = 1
        `, [id]);
        const atual = rows[0];
        if (!atual) throw new MapaDesenhosError("Marcação nao encontrada.", 404);

        const payload = obterPayloadDesenho(payloadOrigem, atual, {
            dataReferenciaPadrao: dataReferencia,
            tecnicosValidos
        });
        validarPayloadDesenho(payload);

        await conn.query(`
            UPDATE ${TABELA_DESENHOS}
            SET titulo = ?, tipo = ?, ferramenta = ?, area_tipo = ?, geometria_json = ?, texto = ?,
                cor = ?, espessura = ?, opacidade = ?, preenchimento_cor = ?, preenchimento_opacidade = ?,
                cor_borda = ?, opacidade_borda = ?, tecnicos_vinculados_json = ?, equipe_vinculada = ?,
                data_referencia = ?, permanente = ?, visibilidade = ?, atualizado_por = ?
            WHERE id = ? AND ativo = 1
        `, [
            payload.titulo,
            payload.tipo,
            payload.ferramenta,
            payload.areaTipo,
            JSON.stringify(payload.geometriaJson),
            payload.texto || null,
            payload.cor,
            payload.espessura,
            payload.opacidade,
            payload.preenchimentoCor,
            payload.preenchimentoOpacidade,
            payload.corBorda,
            payload.opacidadeBorda,
            JSON.stringify(payload.tecnicosVinculados),
            payload.equipeVinculada || null,
            payload.dataReferencia || null,
            payload.permanente ? 1 : 0,
            payload.visibilidade,
            atualizadoPor || null,
            id
        ]);

        const atualizados = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE id = ?
        `, [id]);

        return serializarDesenhoMapa(atualizados[0], tecnicosValidos);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreDesenhos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id && registro.ativo !== false);
    if (!item) throw new MapaDesenhosError("Marcação nao encontrada.", 404);

    const payload = obterPayloadDesenho(payloadOrigem, item, {
        dataReferenciaPadrao: dataReferencia,
        tecnicosValidos
    });
    validarPayloadDesenho(payload);

    item.titulo = payload.titulo;
    item.tipo = payload.tipo;
    item.ferramenta = payload.ferramenta;
    item.area_tipo = payload.areaTipo;
    item.geometria_json = payload.geometriaJson;
    item.texto = payload.texto;
    item.cor = payload.cor;
    item.espessura = payload.espessura;
    item.opacidade = payload.opacidade;
    item.preenchimento_cor = payload.preenchimentoCor;
    item.preenchimento_opacidade = payload.preenchimentoOpacidade;
    item.cor_borda = payload.corBorda;
    item.opacidade_borda = payload.opacidadeBorda;
    item.tecnicos_vinculados_json = payload.tecnicosVinculados;
    item.equipe_vinculada = payload.equipeVinculada;
    item.data_referencia = payload.dataReferencia;
    item.permanente = payload.permanente;
    item.visibilidade = payload.visibilidade;
    item.atualizado_por = atualizadoPor || null;
    item.updated_at = new Date().toISOString();

    salvarStoreDesenhos(store, tecnicosValidos);

    return serializarDesenhoMapa(item, tecnicosValidos);
}

export async function removerDesenhoMapa(idOrigem, { tecnicosValidos = new Set(), removidoPor = null } = {}) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new MapaDesenhosError("Marcação invalida.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE id = ? AND ativo = 1
        `, [id]);
        const atual = rows[0];
        if (!atual) throw new MapaDesenhosError("Marcação nao encontrada.", 404);

        await conn.query(`
            UPDATE ${TABELA_DESENHOS}
            SET ativo = 0, removido_por = ?, removido_em = NOW(), atualizado_por = ?
            WHERE id = ?
        `, [removidoPor || null, removidoPor || null, id]);

        return serializarDesenhoMapa({
            ...atual,
            ativo: 0,
            removido_por: removidoPor || null,
            removido_em: new Date().toISOString()
        }, tecnicosValidos);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreDesenhos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id && registro.ativo !== false);
    if (!item) throw new MapaDesenhosError("Marcação nao encontrada.", 404);

    item.ativo = false;
    item.removido_por = removidoPor || null;
    item.removido_em = new Date().toISOString();
    item.atualizado_por = removidoPor || null;
    item.updated_at = item.removido_em;

    salvarStoreDesenhos(store, tecnicosValidos);

    return serializarDesenhoMapa(item, tecnicosValidos);
}

export async function salvarDesenhoDefinitivoMapa(idOrigem, { tecnicosValidos = new Set(), atualizadoPor = null } = {}) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new MapaDesenhosError("Marcação invalida.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE id = ? AND ativo = 1
        `, [id]);
        if (!rows[0]) throw new MapaDesenhosError("Marcação nao encontrada.", 404);

        await conn.query(`
            UPDATE ${TABELA_DESENHOS}
            SET tipo = 'permanente', permanente = 1, data_referencia = NULL, atualizado_por = ?
            WHERE id = ? AND ativo = 1
        `, [atualizadoPor || null, id]);

        const atualizados = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE id = ?
        `, [id]);

        return serializarDesenhoMapa(atualizados[0], tecnicosValidos);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreDesenhos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id && registro.ativo !== false);
    if (!item) throw new MapaDesenhosError("Marcação nao encontrada.", 404);

    item.tipo = "permanente";
    item.permanente = true;
    item.data_referencia = null;
    item.atualizado_por = atualizadoPor || null;
    item.updated_at = new Date().toISOString();
    salvarStoreDesenhos(store, tecnicosValidos);

    return serializarDesenhoMapa(item, tecnicosValidos);
}

export async function limparDesenhosDiaMapa(dataReferenciaOrigem, { tecnicosValidos = new Set(), removidoPor = null } = {}) {
    const dataReferencia = normalizarDataReferencia(dataReferenciaOrigem);
    if (!dataReferencia) throw new MapaDesenhosError("Informe uma data valida para limpar os desenhos do dia.", 400);

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, titulo, tipo, ferramenta, area_tipo, geometria_json, texto, cor, espessura, opacidade,
                   preenchimento_cor, preenchimento_opacidade, cor_borda, opacidade_borda,
                   tecnicos_vinculados_json, equipe_vinculada, data_referencia, permanente, visibilidade,
                   criado_por, atualizado_por, removido_por, removido_em, ativo, created_at, updated_at
            FROM ${TABELA_DESENHOS}
            WHERE ativo = 1 AND permanente = 0 AND tipo = 'temporario_dia' AND data_referencia = ?
        `, [dataReferencia]);

        await conn.query(`
            UPDATE ${TABELA_DESENHOS}
            SET ativo = 0, removido_por = ?, removido_em = NOW(), atualizado_por = ?
            WHERE ativo = 1 AND permanente = 0 AND tipo = 'temporario_dia' AND data_referencia = ?
        `, [removidoPor || null, removidoPor || null, dataReferencia]);

        return rows.map(row => serializarDesenhoMapa({
            ...row,
            ativo: 0,
            removido_por: removidoPor || null,
            removido_em: new Date().toISOString()
        }, tecnicosValidos));
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreDesenhos(tecnicosValidos);
    const removidos = [];
    const agora = new Date().toISOString();

    store.items.forEach(item => {
        const desenho = serializarDesenhoMapa(item, tecnicosValidos);
        if (!desenho.ativo || desenho.permanente || desenho.tipo !== "temporario_dia" || desenho.dataReferencia !== dataReferencia) return;

        item.ativo = false;
        item.removido_por = removidoPor || null;
        item.removido_em = agora;
        item.atualizado_por = removidoPor || null;
        item.updated_at = agora;
        removidos.push(serializarDesenhoMapa(item, tecnicosValidos));
    });

    salvarStoreDesenhos(store, tecnicosValidos);

    return removidos;
}
