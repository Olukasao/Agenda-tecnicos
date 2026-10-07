import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const AGENDA_LUNCH_PERMISSION = "agenda_tecnicos_gerenciar_almoco";

const TABELA_ALMOCOS = "agenda_tecnicos_almocos";
const TIPO_ALMOCO_PADRAO = "almoco";
const TIPOS_BLOQUEIO_AGENDA = new Set([
    "almoco",
    "compromisso",
    "reuniao",
    "treinamento",
    "indisponivel",
    "outro"
]);
const DESCRICAO_BLOQUEIO_MAX = 255;
const HORA_INICIO_AGENDA_MINUTOS = 8 * 60;
const HORA_FIM_AGENDA_MINUTOS = 24 * 60;
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ARQUIVO_ALMOCOS = path.join(__dirname, "..", "data", "agenda_tecnicos_almocos.json");

let mariaDbPoolPromise = null;
let tabelaMariaDbGarantida = false;
let mariaDbAvisoEmitido = false;

export class AgendaLunchError extends Error {
    constructor(message, status = 400) {
        super(message);
        this.name = "AgendaLunchError";
        this.status = status;
    }
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
    console.warn("⚠️ MariaDB indisponível para almoços; usando store local persistente.", err?.message || err);
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
        CREATE TABLE IF NOT EXISTS ${TABELA_ALMOCOS} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            tecnico_id VARCHAR(80) NOT NULL,
            data DATE NOT NULL,
            tipo VARCHAR(32) NOT NULL DEFAULT 'almoco',
            descricao VARCHAR(255) NULL,
            hora_inicio TIME NOT NULL,
            hora_fim TIME NOT NULL,
            criado_por VARCHAR(120) NULL,
            criado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            atualizado_em DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_almoco_tecnico_data (tecnico_id, data),
            INDEX idx_almoco_data (data)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    const colunas = await conn.query(`SHOW COLUMNS FROM ${TABELA_ALMOCOS}`);
    const nomesColunas = new Set(colunas.map(coluna => String(coluna.Field || "").toLowerCase()));
    const alteracoes = [];

    if (!nomesColunas.has("tipo")) {
        alteracoes.push("ADD COLUMN tipo VARCHAR(32) NOT NULL DEFAULT 'almoco' AFTER data");
    }

    if (!nomesColunas.has("descricao")) {
        alteracoes.push("ADD COLUMN descricao VARCHAR(255) NULL AFTER tipo");
    }

    if (alteracoes.length) {
        await conn.query(`ALTER TABLE ${TABELA_ALMOCOS} ${alteracoes.join(", ")}`);
    }

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
        if (err instanceof AgendaLunchError) throw err;
        avisarMariaDbIndisponivel(err);
        return null;
    } finally {
        if (conn) conn.release();
    }
}

function criarStoreAlmocosVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function garantirArquivoAlmocos() {
    const dir = path.dirname(ARQUIVO_ALMOCOS);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(ARQUIVO_ALMOCOS)) {
        fs.writeFileSync(ARQUIVO_ALMOCOS, JSON.stringify(criarStoreAlmocosVazio(), null, 2));
    }
}

function normalizarDataSaida(valor) {
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) {
        return valor.toISOString().slice(0, 10);
    }

    return String(valor || "").slice(0, 10);
}

function normalizarHoraSaida(valor) {
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) {
        return valor.toISOString().slice(11, 16);
    }

    return String(valor || "").slice(0, 5);
}

export function normalizarDataAlmoco(data) {
    const valor = String(data || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(valor)) return "";

    const [ano, mes, dia] = valor.split("-").map(Number);
    const dataUtc = new Date(Date.UTC(ano, mes - 1, dia));
    if (
        dataUtc.getUTCFullYear() !== ano
        || dataUtc.getUTCMonth() !== mes - 1
        || dataUtc.getUTCDate() !== dia
    ) {
        return "";
    }

    return valor;
}

export function normalizarHoraAlmoco(hora) {
    const valor = String(hora || "").trim();
    const match = valor.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (!match) return "";

    const horas = Number(match[1]);
    const minutos = Number(match[2]);
    const segundos = Number(match[3] || 0);

    if (!Number.isInteger(horas) || !Number.isInteger(minutos) || !Number.isInteger(segundos)) return "";
    if (horas < 0 || horas > 24 || minutos < 0 || minutos > 59 || segundos < 0 || segundos > 59) return "";
    if (horas === 24 && (minutos !== 0 || segundos !== 0)) return "";

    return `${String(horas).padStart(2, "0")}:${String(minutos).padStart(2, "0")}`;
}

export function normalizarTipoBloqueioAgenda(tipo) {
    const valor = String(tipo || "").trim().toLowerCase();
    if (!valor) return TIPO_ALMOCO_PADRAO;
    if (valor === "reunião") return "reuniao";
    if (valor === "indisponível") return "indisponivel";
    if (!TIPOS_BLOQUEIO_AGENDA.has(valor)) return "";

    return valor;
}

function normalizarDescricaoBloqueio(descricao) {
    const valor = String(descricao || "").trim().replace(/\s+/g, " ");
    if (!valor) return null;

    return valor.slice(0, DESCRICAO_BLOQUEIO_MAX);
}

export function minutosDeHora(hora) {
    const normalizada = normalizarHoraAlmoco(hora);
    if (!normalizada) return null;

    const [horas, minutos] = normalizada.split(":").map(Number);
    return (horas * 60) + minutos;
}

function horaComSegundos(hora) {
    const normalizada = normalizarHoraAlmoco(hora);
    return normalizada ? `${normalizada}:00` : "";
}

function serializarAlmoco(registro) {
    const data = normalizarDataSaida(registro.data || registro.lunch_date);
    const horaInicio = normalizarHoraSaida(registro.hora_inicio || registro.horaInicio);
    const horaFim = normalizarHoraSaida(registro.hora_fim || registro.horaFim);
    const tipo = normalizarTipoBloqueioAgenda(registro.tipo || registro.tipo_bloqueio) || TIPO_ALMOCO_PADRAO;
    const descricao = tipo === TIPO_ALMOCO_PADRAO
        ? null
        : normalizarDescricaoBloqueio(registro.descricao || registro.description);

    return {
        id: Number(registro.id),
        tecnicoId: String(registro.tecnico_id || registro.tecnicoId || ""),
        tecnico_id: String(registro.tecnico_id || registro.tecnicoId || ""),
        data,
        tipo,
        descricao,
        horaInicio,
        horaFim,
        hora_inicio: horaInicio,
        hora_fim: horaFim,
        criadoPor: registro.criado_por || registro.criadoPor || null,
        criado_por: registro.criado_por || registro.criadoPor || null,
        criadoEm: registro.criado_em || registro.criadoEm || registro.created_at || null,
        criado_em: registro.criado_em || registro.criadoEm || registro.created_at || null,
        atualizadoEm: registro.atualizado_em || registro.atualizadoEm || registro.updated_at || null,
        atualizado_em: registro.atualizado_em || registro.atualizadoEm || registro.updated_at || null
    };
}

function normalizarStoreAlmocos(store, tecnicosValidos = new Set()) {
    const items = (Array.isArray(store?.items) ? store.items : [])
        .map((item, index) => {
            const tipo = normalizarTipoBloqueioAgenda(item.tipo || item.tipo_bloqueio) || TIPO_ALMOCO_PADRAO;

            return {
                id: Number(item.id) || index + 1,
                tecnico_id: String(item.tecnico_id || item.tecnicoId || "").trim(),
                data: normalizarDataAlmoco(item.data),
                tipo,
                descricao: tipo === TIPO_ALMOCO_PADRAO ? null : normalizarDescricaoBloqueio(item.descricao || item.description),
                hora_inicio: normalizarHoraAlmoco(item.hora_inicio || item.horaInicio),
                hora_fim: normalizarHoraAlmoco(item.hora_fim || item.horaFim),
                criado_por: item.criado_por || item.criadoPor || null,
                criado_em: item.criado_em || item.criadoEm || item.created_at || new Date().toISOString(),
                atualizado_em: item.atualizado_em || item.atualizadoEm || item.updated_at || new Date().toISOString()
            };
        })
        .filter(item => item.tecnico_id && (!tecnicosValidos.size || tecnicosValidos.has(item.tecnico_id)))
        .filter(item => item.data && item.hora_inicio && item.hora_fim)
        .filter(item => {
            const inicio = minutosDeHora(item.hora_inicio);
            const fim = minutosDeHora(item.hora_fim);
            return inicio !== null && fim !== null && fim > inicio;
        });

    const maiorId = items.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = items.length === 0
        ? 1
        : Number.isInteger(store?.nextId) && store.nextId > maiorId
            ? store.nextId
            : maiorId + 1;

    return { nextId, items };
}

function lerStoreAlmocos(tecnicosValidos) {
    garantirArquivoAlmocos();
    const conteudo = fs.readFileSync(ARQUIVO_ALMOCOS, "utf8");
    return normalizarStoreAlmocos(JSON.parse(conteudo || "{}"), tecnicosValidos);
}

function salvarStoreAlmocos(store, tecnicosValidos) {
    garantirArquivoAlmocos();
    const normalizado = normalizarStoreAlmocos(store, tecnicosValidos);
    const temporario = `${ARQUIVO_ALMOCOS}.tmp`;

    fs.writeFileSync(temporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(temporario, ARQUIVO_ALMOCOS);

    return normalizado;
}

function obterPayloadAlmoco(payload = {}, registroAtual = {}) {
    const tipo = normalizarTipoBloqueioAgenda(
        payload.tipo
        || payload.type
        || registroAtual.tipo
        || TIPO_ALMOCO_PADRAO
    );
    const descricaoOrigem = Object.prototype.hasOwnProperty.call(payload, "descricao")
        ? payload.descricao
        : Object.prototype.hasOwnProperty.call(payload, "description")
            ? payload.description
            : registroAtual.descricao;

    return {
        tecnicoId: String(payload.tecnicoId || payload.tecnico_id || registroAtual.tecnico_id || "").trim(),
        data: normalizarDataAlmoco(payload.data || registroAtual.data),
        tipo,
        descricao: tipo === TIPO_ALMOCO_PADRAO
            ? null
            : normalizarDescricaoBloqueio(descricaoOrigem),
        horaInicio: normalizarHoraAlmoco(payload.horaInicio || payload.hora_inicio || registroAtual.hora_inicio),
        horaFim: normalizarHoraAlmoco(payload.horaFim || payload.hora_fim || registroAtual.hora_fim)
    };
}

function validarPayloadAlmoco(payload, tecnicosValidos) {
    if (!payload.tecnicoId || !tecnicosValidos.has(payload.tecnicoId)) {
        throw new AgendaLunchError("Técnico não encontrado.", 404);
    }

    if (!payload.data) {
        throw new AgendaLunchError("Informe uma data válida no formato YYYY-MM-DD.", 400);
    }

    if (!payload.tipo) {
        throw new AgendaLunchError("Informe um tipo de bloqueio válido.", 400);
    }

    if (!payload.horaInicio || !payload.horaFim) {
        throw new AgendaLunchError("Informe os horários no formato HH:MM.", 400);
    }

    const inicio = minutosDeHora(payload.horaInicio);
    const fim = minutosDeHora(payload.horaFim);

    if (inicio === null || fim === null) {
        throw new AgendaLunchError("Informe os horários no formato HH:MM.", 400);
    }

    if (fim <= inicio) {
        throw new AgendaLunchError("O horário final precisa ser maior que o horário inicial.", 400);
    }

    if (inicio < HORA_INICIO_AGENDA_MINUTOS || fim > HORA_FIM_AGENDA_MINUTOS) {
        throw new AgendaLunchError("O horário informado está fora do intervalo exibido pela agenda.", 400);
    }
}

function temConflitoAlmoco(item, payload, idIgnorado = null) {
    if (idIgnorado !== null && Number(item.id) === Number(idIgnorado)) return false;
    if (String(item.tecnico_id) !== String(payload.tecnicoId)) return false;
    if (normalizarDataSaida(item.data) !== payload.data) return false;

    const inicioExistente = minutosDeHora(item.hora_inicio);
    const fimExistente = minutosDeHora(item.hora_fim);
    const inicioNovo = minutosDeHora(payload.horaInicio);
    const fimNovo = minutosDeHora(payload.horaFim);

    return inicioExistente < fimNovo && fimExistente > inicioNovo;
}

function validarConflitoLocal(items, payload, idIgnorado = null) {
    if (items.some(item => temConflitoAlmoco(item, payload, idIgnorado))) {
        const mensagem = payload.tipo === TIPO_ALMOCO_PADRAO
            ? "Este técnico já possui um horário de almoço nesse período."
            : "Este técnico já possui um bloqueio nesse período.";
        throw new AgendaLunchError(mensagem, 409);
    }
}

async function validarConflitoMariaDb(conn, payload, idIgnorado = null) {
    const params = [
        payload.tecnicoId,
        payload.data,
        horaComSegundos(payload.horaFim),
        horaComSegundos(payload.horaInicio)
    ];
    let filtroId = "";

    if (idIgnorado !== null) {
        filtroId = " AND id <> ?";
        params.push(Number(idIgnorado));
    }

    const conflitos = await conn.query(`
        SELECT id
        FROM ${TABELA_ALMOCOS}
        WHERE tecnico_id = ?
          AND data = ?
          AND hora_inicio < ?
          AND hora_fim > ?
          ${filtroId}
        LIMIT 1
    `, params);

    if (conflitos.length) {
        const mensagem = payload.tipo === TIPO_ALMOCO_PADRAO
            ? "Este técnico já possui um horário de almoço nesse período."
            : "Este técnico já possui um bloqueio nesse período.";
        throw new AgendaLunchError(mensagem, 409);
    }
}

async function listarAlmocosMariaDb(conn, data) {
    const rows = await conn.query(`
        SELECT id, tecnico_id, data, tipo, descricao, hora_inicio, hora_fim, criado_por, criado_em, atualizado_em
        FROM ${TABELA_ALMOCOS}
        WHERE data = ?
        ORDER BY tecnico_id ASC, hora_inicio ASC
    `, [data]);

    return rows.map(serializarAlmoco);
}

function listarAlmocosLocal(data, tecnicosValidos) {
    return lerStoreAlmocos(tecnicosValidos).items
        .filter(item => item.data === data)
        .sort((a, b) => (
            a.tecnico_id.localeCompare(b.tecnico_id, "pt-BR")
            || a.hora_inicio.localeCompare(b.hora_inicio)
        ))
        .map(serializarAlmoco);
}

export async function listarAlmocosAgenda(data, tecnicosValidos) {
    const dataNormalizada = normalizarDataAlmoco(data);
    if (!dataNormalizada) {
        throw new AgendaLunchError("Informe uma data válida no formato YYYY-MM-DD.", 400);
    }

    const resultadoDb = await executarComMariaDb(conn => listarAlmocosMariaDb(conn, dataNormalizada));
    if (resultadoDb) return resultadoDb.valor;

    return listarAlmocosLocal(dataNormalizada, tecnicosValidos);
}

export async function criarAlmocoAgenda(payloadOrigem, { tecnicosValidos, criadoPor }) {
    const payload = obterPayloadAlmoco(payloadOrigem);
    validarPayloadAlmoco(payload, tecnicosValidos);

    const resultadoDb = await executarComMariaDb(async (conn) => {
        await validarConflitoMariaDb(conn, payload);
        const resultado = await conn.query(`
            INSERT INTO ${TABELA_ALMOCOS}
                (tecnico_id, data, tipo, descricao, hora_inicio, hora_fim, criado_por)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        `, [
            payload.tecnicoId,
            payload.data,
            payload.tipo,
            payload.descricao,
            horaComSegundos(payload.horaInicio),
            horaComSegundos(payload.horaFim),
            criadoPor || null
        ]);
        const id = Number(resultado.insertId);
        const rows = await conn.query(`
            SELECT id, tecnico_id, data, tipo, descricao, hora_inicio, hora_fim, criado_por, criado_em, atualizado_em
            FROM ${TABELA_ALMOCOS}
            WHERE id = ?
        `, [id]);

        return serializarAlmoco(rows[0]);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreAlmocos(tecnicosValidos);
    validarConflitoLocal(store.items, payload);

    const agora = new Date().toISOString();
    const item = {
        id: store.nextId,
        tecnico_id: payload.tecnicoId,
        data: payload.data,
        tipo: payload.tipo,
        descricao: payload.descricao,
        hora_inicio: payload.horaInicio,
        hora_fim: payload.horaFim,
        criado_por: criadoPor || null,
        criado_em: agora,
        atualizado_em: agora
    };

    store.nextId += 1;
    store.items.push(item);
    salvarStoreAlmocos(store, tecnicosValidos);

    return serializarAlmoco(item);
}

export async function atualizarAlmocoAgenda(idOrigem, payloadOrigem, { tecnicosValidos }) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new AgendaLunchError("Marcação de almoço inválida.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, tecnico_id, data, tipo, descricao, hora_inicio, hora_fim, criado_por, criado_em, atualizado_em
            FROM ${TABELA_ALMOCOS}
            WHERE id = ?
        `, [id]);
        const atual = rows[0];

        if (!atual) throw new AgendaLunchError("Almoço não encontrado.", 404);

        const payload = obterPayloadAlmoco(payloadOrigem, serializarAlmoco(atual));
        validarPayloadAlmoco(payload, tecnicosValidos);
        await validarConflitoMariaDb(conn, payload, id);

        await conn.query(`
            UPDATE ${TABELA_ALMOCOS}
            SET tecnico_id = ?, data = ?, tipo = ?, descricao = ?, hora_inicio = ?, hora_fim = ?
            WHERE id = ?
        `, [
            payload.tecnicoId,
            payload.data,
            payload.tipo,
            payload.descricao,
            horaComSegundos(payload.horaInicio),
            horaComSegundos(payload.horaFim),
            id
        ]);

        const atualizados = await conn.query(`
            SELECT id, tecnico_id, data, tipo, descricao, hora_inicio, hora_fim, criado_por, criado_em, atualizado_em
            FROM ${TABELA_ALMOCOS}
            WHERE id = ?
        `, [id]);

        return serializarAlmoco(atualizados[0]);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreAlmocos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id);
    if (!item) throw new AgendaLunchError("Almoço não encontrado.", 404);

    const payload = obterPayloadAlmoco(payloadOrigem, item);
    validarPayloadAlmoco(payload, tecnicosValidos);
    validarConflitoLocal(store.items, payload, id);

    item.tecnico_id = payload.tecnicoId;
    item.data = payload.data;
    item.tipo = payload.tipo;
    item.descricao = payload.descricao;
    item.hora_inicio = payload.horaInicio;
    item.hora_fim = payload.horaFim;
    item.atualizado_em = new Date().toISOString();

    salvarStoreAlmocos(store, tecnicosValidos);

    return serializarAlmoco(item);
}

export async function removerAlmocoAgenda(idOrigem, tecnicosValidos) {
    const id = Number(idOrigem);
    if (!Number.isInteger(id) || id <= 0) {
        throw new AgendaLunchError("Marcação de almoço inválida.", 400);
    }

    const resultadoDb = await executarComMariaDb(async (conn) => {
        const rows = await conn.query(`
            SELECT id, tecnico_id, data, tipo, descricao, hora_inicio, hora_fim, criado_por, criado_em, atualizado_em
            FROM ${TABELA_ALMOCOS}
            WHERE id = ?
        `, [id]);
        const atual = rows[0];

        if (!atual) throw new AgendaLunchError("Almoço não encontrado.", 404);

        await conn.query(`DELETE FROM ${TABELA_ALMOCOS} WHERE id = ?`, [id]);
        return serializarAlmoco(atual);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreAlmocos(tecnicosValidos);
    const item = store.items.find(registro => Number(registro.id) === id);
    if (!item) throw new AgendaLunchError("Almoço não encontrado.", 404);

    store.items = store.items.filter(registro => Number(registro.id) !== id);
    salvarStoreAlmocos(store, tecnicosValidos);

    return serializarAlmoco(item);
}
