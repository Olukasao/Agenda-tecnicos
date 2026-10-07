import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const ARQUIVO_HORARIOS_SEMANA = path.join(__dirname, "..", "data", "agenda_tecnicos_horarios_semana.json");
const ARQUIVO_HORARIOS_LEGADO = path.join(__dirname, "..", "data", "tecnico_horarios.json");
const SEMANA_REFERENCIA_PADRAO = "2026-06-22";
const TIMEZONE_AGENDA = "America/Sao_Paulo";
const TURNO_SEM_ATRIBUICAO_ID = "__sem_turno__";
const TIPO_TURNO_ALTERNANCIA = "alternancia";
const TIPO_TURNO_FIXO = "fixo";
const TIPOS_TURNO = new Set([TIPO_TURNO_ALTERNANCIA, TIPO_TURNO_FIXO]);

const TABELAS = {
    shifts: "schedule_shifts",
    groups: "technician_rotation_groups",
    assignments: "technician_group_assignments",
    rules: "rotation_group_shift_rules",
    shiftRotationRules: "schedule_shift_rotation_rules",
    overrides: "weekly_technician_schedule_overrides",
    fixedAssignments: "fixed_shift_technician_assignments"
};

const TECNICOS_GRUPO_1_INICIAL = ["Alex", "Claudinei", "Nelson", "Ramon"];
const TECNICOS_GRUPO_2_INICIAL = ["Alan", "Paulino", "Emerson", "Lucio"];

let mariaDbPoolPromise = null;
let tabelasMariaDbGarantidas = false;
let mariaDbAvisoEmitido = false;

export class TechnicianScheduleError extends Error {
    constructor(message, status = 400) {
        super(message);
        this.name = "TechnicianScheduleError";
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
        timezone: TIMEZONE_AGENDA
    };
}

function avisarMariaDbIndisponivel(err) {
    if (mariaDbAvisoEmitido) return;
    mariaDbAvisoEmitido = true;
    console.warn("MariaDB indisponível para horários semanais; usando store local persistente.", err?.message || err);
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

async function garantirTabelasMariaDb(conn) {
    if (tabelasMariaDbGarantidas) return;

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.shifts} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            name VARCHAR(80) NOT NULL,
            start_time TIME NOT NULL,
            end_time TIME NOT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            active TINYINT(1) NOT NULL DEFAULT 1,
            tipo_turno VARCHAR(20) NOT NULL DEFAULT 'alternancia',
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_schedule_shifts_active_order (active, sort_order)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.groups} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            name VARCHAR(80) NOT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            active TINYINT(1) NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_rotation_groups_active_order (active, sort_order)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.assignments} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            technician_id VARCHAR(80) NOT NULL,
            rotation_group_id VARCHAR(40) NOT NULL,
            valid_from_week DATE NOT NULL,
            valid_until_week DATE NULL,
            sort_order INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_assignment_technician_from (technician_id, valid_from_week),
            INDEX idx_assignment_lookup (technician_id, valid_from_week, valid_until_week),
            INDEX idx_assignment_group (rotation_group_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.rules} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            rotation_group_id VARCHAR(40) NOT NULL,
            cycle_position INT NOT NULL,
            shift_id VARCHAR(40) NOT NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_rule_group_cycle (rotation_group_id, cycle_position),
            INDEX idx_rule_shift (shift_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.shiftRotationRules} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            shift_id VARCHAR(40) NOT NULL,
            next_shift_id VARCHAR(40) NOT NULL,
            valid_from_week DATE NOT NULL,
            valid_until_week DATE NULL,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_shift_rotation_from (shift_id, valid_from_week),
            INDEX idx_shift_rotation_effective (shift_id, valid_from_week, valid_until_week),
            INDEX idx_shift_rotation_next (next_shift_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.overrides} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            week_start DATE NOT NULL,
            technician_id VARCHAR(80) NOT NULL,
            shift_id VARCHAR(40) NOT NULL,
            rotation_group_id VARCHAR(40) NULL,
            sort_order INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_override_week_technician (week_start, technician_id),
            INDEX idx_override_shift (week_start, shift_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    await conn.query(`
        CREATE TABLE IF NOT EXISTS ${TABELAS.fixedAssignments} (
            id VARCHAR(40) NOT NULL PRIMARY KEY,
            shift_id VARCHAR(40) NOT NULL,
            technician_id VARCHAR(80) NOT NULL,
            sort_order INT NOT NULL DEFAULT 0,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_fixed_shift_technician (shift_id, technician_id),
            INDEX idx_fixed_shift_order (shift_id, sort_order),
            INDEX idx_fixed_technician (technician_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `);

    const shiftTypeColumn = await conn.query(`SHOW COLUMNS FROM ${TABELAS.shifts} LIKE 'tipo_turno'`);
    if (!shiftTypeColumn.length) {
        await conn.query(`ALTER TABLE ${TABELAS.shifts} ADD COLUMN tipo_turno VARCHAR(20) NOT NULL DEFAULT 'alternancia' AFTER active`);
    }

    await conn.query(`
        UPDATE ${TABELAS.shifts}
        SET tipo_turno = 'alternancia'
        WHERE tipo_turno IS NULL OR tipo_turno NOT IN ('alternancia', 'fixo')
    `);

    const overrideGroupColumn = await conn.query(`SHOW COLUMNS FROM ${TABELAS.overrides} LIKE 'rotation_group_id'`);
    if (!overrideGroupColumn.length) {
        await conn.query(`ALTER TABLE ${TABELAS.overrides} ADD COLUMN rotation_group_id VARCHAR(40) NULL AFTER shift_id`);
    }

    tabelasMariaDbGarantidas = true;
}

async function executarComMariaDb(callback) {
    const pool = await obterPoolMariaDb();
    if (!pool) return null;

    let conn;
    try {
        conn = await pool.getConnection();
        await garantirTabelasMariaDb(conn);
        const valor = await callback(conn);
        return { valor };
    } catch (err) {
        if (err instanceof TechnicianScheduleError) throw err;
        avisarMariaDbIndisponivel(err);
        return null;
    } finally {
        if (conn) conn.release();
    }
}

function clonar(valor) {
    return JSON.parse(JSON.stringify(valor));
}

function obterHorariosLegados() {
    const fallback = [
        { id: "turno_1", name: "Turno 1", start_time: "08:00", end_time: "16:48", sort_order: 1, active: true, tipo_turno: TIPO_TURNO_ALTERNANCIA },
        { id: "turno_2", name: "Turno 2", start_time: "09:00", end_time: "19:00", sort_order: 2, active: true, tipo_turno: TIPO_TURNO_ALTERNANCIA }
    ];

    try {
        if (!fs.existsSync(ARQUIVO_HORARIOS_LEGADO)) return fallback;
        const conteudo = fs.readFileSync(ARQUIVO_HORARIOS_LEGADO, "utf8");
        const store = JSON.parse(conteudo || "{}");
        const items = Array.isArray(store?.items) ? store.items : [];
        const horario08 = items.find(item => item?.id === "horario_08") || {};
        const horario09 = items.find(item => item?.id === "horario_09") || {};

        return [
            {
                ...fallback[0],
                start_time: normalizarHora(horario08.entrada, fallback[0].start_time),
                end_time: normalizarHora(horario08.saida, fallback[0].end_time)
            },
            {
                ...fallback[1],
                start_time: normalizarHora(horario09.entrada, fallback[1].start_time),
                end_time: normalizarHora(horario09.saida, fallback[1].end_time)
            }
        ];
    } catch {
        return fallback;
    }
}

function criarStoreInicial(tecnicosValidos = new Set()) {
    const shifts = obterHorariosLegados();
    const grupos = [
        { id: "grupo_1", name: "Grupo 1", sort_order: 1, active: true },
        { id: "grupo_2", name: "Grupo 2", sort_order: 2, active: true }
    ];
    const tecnicosAtivos = tecnicosValidos instanceof Set && tecnicosValidos.size
        ? tecnicosValidos
        : new Set([...TECNICOS_GRUPO_1_INICIAL, ...TECNICOS_GRUPO_2_INICIAL]);
    const now = new Date().toISOString();
    const assignments = [];

    TECNICOS_GRUPO_1_INICIAL.forEach((technicianId, index) => {
        if (!tecnicosAtivos.has(technicianId)) return;
        assignments.push({
            id: `atribuicao_inicial_g1_${index + 1}`,
            technician_id: technicianId,
            rotation_group_id: "grupo_1",
            valid_from_week: SEMANA_REFERENCIA_PADRAO,
            valid_until_week: null,
            sort_order: index + 1,
            created_at: now,
            updated_at: now
        });
    });

    TECNICOS_GRUPO_2_INICIAL.forEach((technicianId, index) => {
        if (!tecnicosAtivos.has(technicianId)) return;
        assignments.push({
            id: `atribuicao_inicial_g2_${index + 1}`,
            technician_id: technicianId,
            rotation_group_id: "grupo_2",
            valid_from_week: SEMANA_REFERENCIA_PADRAO,
            valid_until_week: null,
            sort_order: index + 1,
            created_at: now,
            updated_at: now
        });
    });

    return {
        referenceWeek: SEMANA_REFERENCIA_PADRAO,
        nextIds: {
            shift: 3,
            group: 3,
            assignment: assignments.length + 1,
            rule: 5,
            shiftRotationRule: 3,
            override: 1,
            fixedAssignment: 1
        },
        shifts,
        rotationGroups: grupos,
        groupAssignments: assignments,
        rotationRules: [
            { id: "regra_g1_0", rotation_group_id: "grupo_1", cycle_position: 0, shift_id: "turno_1", created_at: now, updated_at: now },
            { id: "regra_g1_1", rotation_group_id: "grupo_1", cycle_position: 1, shift_id: "turno_2", created_at: now, updated_at: now },
            { id: "regra_g2_0", rotation_group_id: "grupo_2", cycle_position: 0, shift_id: "turno_2", created_at: now, updated_at: now },
            { id: "regra_g2_1", rotation_group_id: "grupo_2", cycle_position: 1, shift_id: "turno_1", created_at: now, updated_at: now }
        ],
        shiftRotationRules: [
            { id: "rotacao_turno_1", shift_id: "turno_1", next_shift_id: "turno_2", valid_from_week: SEMANA_REFERENCIA_PADRAO, valid_until_week: null, created_at: now, updated_at: now },
            { id: "rotacao_turno_2", shift_id: "turno_2", next_shift_id: "turno_1", valid_from_week: SEMANA_REFERENCIA_PADRAO, valid_until_week: null, created_at: now, updated_at: now }
        ],
        fixedShiftAssignments: [],
        weeklyOverrides: [],
        updated_at: now
    };
}

function garantirArquivoHorariosSemana(tecnicosValidos = new Set()) {
    const dir = path.dirname(ARQUIVO_HORARIOS_SEMANA);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(ARQUIVO_HORARIOS_SEMANA)) {
        fs.writeFileSync(ARQUIVO_HORARIOS_SEMANA, JSON.stringify(criarStoreInicial(tecnicosValidos), null, 2));
    }
}

function normalizarData(valor) {
    const texto = String(valor || "").trim().slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(texto)) return "";

    const [ano, mes, dia] = texto.split("-").map(Number);
    const dataUtc = new Date(Date.UTC(ano, mes - 1, dia));
    if (
        dataUtc.getUTCFullYear() !== ano
        || dataUtc.getUTCMonth() !== mes - 1
        || dataUtc.getUTCDate() !== dia
    ) {
        return "";
    }

    return texto;
}

function normalizarDataSaida(valor) {
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) {
        return [
            valor.getFullYear(),
            String(valor.getMonth() + 1).padStart(2, "0"),
            String(valor.getDate()).padStart(2, "0")
        ].join("-");
    }

    return normalizarData(valor);
}

function dataParaUtc(dataISO) {
    const data = normalizarData(dataISO);
    if (!data) return null;
    const [ano, mes, dia] = data.split("-").map(Number);
    return new Date(Date.UTC(ano, mes - 1, dia));
}

function formatarDataUtc(data) {
    return [
        data.getUTCFullYear(),
        String(data.getUTCMonth() + 1).padStart(2, "0"),
        String(data.getUTCDate()).padStart(2, "0")
    ].join("-");
}

function adicionarDiasData(dataISO, dias) {
    const data = dataParaUtc(dataISO);
    if (!data) return "";
    data.setUTCDate(data.getUTCDate() + dias);
    return formatarDataUtc(data);
}

export function obterInicioSemana(dataReferencia) {
    const data = dataParaUtc(dataReferencia);
    if (!data) return "";

    const dia = data.getUTCDay();
    const diferenca = dia === 0 ? -6 : 1 - dia;
    data.setUTCDate(data.getUTCDate() + diferenca);
    return formatarDataUtc(data);
}

export function obterFimSemana(weekStart) {
    return adicionarDiasData(weekStart, 6);
}

export function normalizarDataAgenda(valor) {
    return normalizarData(valor);
}

export function normalizarHora(valor, fallback = "") {
    const texto = String(valor || "").trim();
    const match = texto.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
    if (!match) return fallback;

    const hora = Number(match[1]);
    const minuto = Number(match[2]);
    const segundo = Number(match[3] || 0);

    if (!Number.isInteger(hora) || !Number.isInteger(minuto) || !Number.isInteger(segundo)) return fallback;
    if (hora < 0 || hora > 23 || minuto < 0 || minuto > 59 || segundo < 0 || segundo > 59) return fallback;

    return `${String(hora).padStart(2, "0")}:${String(minuto).padStart(2, "0")}`;
}

function horaComSegundos(valor) {
    const hora = normalizarHora(valor);
    return hora ? `${hora}:00` : "";
}

function normalizarHoraSaida(valor) {
    if (valor instanceof Date && !Number.isNaN(valor.getTime())) {
        return valor.toISOString().slice(11, 16);
    }

    return normalizarHora(String(valor || "").slice(0, 8));
}

function normalizarBooleano(valor, fallback = true) {
    if (valor === true || valor === 1 || valor === "1" || valor === "true" || valor === "sim") return true;
    if (valor === false || valor === 0 || valor === "0" || valor === "false" || valor === "nao") return false;
    return fallback;
}

function normalizarTipoTurno(valor, fallback = TIPO_TURNO_ALTERNANCIA) {
    const texto = String(valor || "").trim().toLowerCase();
    if (TIPOS_TURNO.has(texto)) return texto;
    return fallback;
}

function normalizarTipoTurnoObrigatorio(valor) {
    const texto = String(valor || "").trim().toLowerCase();
    if (TIPOS_TURNO.has(texto)) return texto;
    throw new TechnicianScheduleError("Tipo de turno inválido.", 400);
}

function isTurnoFixo(turno) {
    return normalizarTipoTurno(turno?.tipo_turno || turno?.tipoTurno) === TIPO_TURNO_FIXO;
}

function isTurnoAlternancia(turno) {
    return !isTurnoFixo(turno);
}

function isTurnoSemAtribuicao(shiftId) {
    return String(shiftId || "").trim() === TURNO_SEM_ATRIBUICAO_ID;
}

function obterHojeBrasil() {
    const partes = new Intl.DateTimeFormat("pt-BR", {
        timeZone: TIMEZONE_AGENDA,
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    }).formatToParts(new Date()).reduce((acc, parte) => {
        acc[parte.type] = parte.value;
        return acc;
    }, {});

    return `${partes.year}-${partes.month}-${partes.day}`;
}

function ordenarPorOrdemENome(a, b) {
    const ordem = Number(a.sort_order ?? a.sortOrder ?? 0) - Number(b.sort_order ?? b.sortOrder ?? 0);
    if (ordem !== 0) return ordem;
    return String(a.name || a.technician_id || "").localeCompare(String(b.name || b.technician_id || ""), "pt-BR");
}

function criarRegrasRotacaoTurnosPadrao(shifts = [], rotationRules = [], referenceWeek = SEMANA_REFERENCIA_PADRAO) {
    const shiftsAlternancia = shifts.filter(isTurnoAlternancia);
    const shiftIds = new Set(shiftsAlternancia.map(item => item.id));
    const votosPorTurno = new Map();
    const regrasPorGrupo = new Map();
    const now = new Date().toISOString();

    rotationRules.forEach(rule => {
        if (!rule.rotation_group_id || !shiftIds.has(rule.shift_id)) return;
        if (!regrasPorGrupo.has(rule.rotation_group_id)) {
            regrasPorGrupo.set(rule.rotation_group_id, []);
        }
        regrasPorGrupo.get(rule.rotation_group_id).push(rule);
    });

    regrasPorGrupo.forEach(regras => {
        const ordenadas = [...regras].sort((a, b) => Number(a.cycle_position) - Number(b.cycle_position));
        if (!ordenadas.length) return;

        ordenadas.forEach((regra, index) => {
            const proxima = ordenadas[(index + 1) % ordenadas.length];
            if (!regra?.shift_id || !proxima?.shift_id) return;
            if (!shiftIds.has(regra.shift_id) || !shiftIds.has(proxima.shift_id)) return;

            if (!votosPorTurno.has(regra.shift_id)) votosPorTurno.set(regra.shift_id, new Map());
            const votosDestino = votosPorTurno.get(regra.shift_id);
            votosDestino.set(proxima.shift_id, (votosDestino.get(proxima.shift_id) || 0) + 1);
        });
    });

    return shiftsAlternancia.map((shift, index) => {
        const votosDestino = votosPorTurno.get(shift.id);
        const nextShiftId = votosDestino
            ? [...votosDestino.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))[0]?.[0]
            : shift.id;

        return {
            id: `rotacao_inicial_${index + 1}`,
            shift_id: shift.id,
            next_shift_id: shiftIds.has(nextShiftId) ? nextShiftId : shift.id,
            valid_from_week: referenceWeek,
            valid_until_week: null,
            created_at: now,
            updated_at: now
        };
    });
}

function normalizarStore(store, tecnicosValidos = new Set()) {
    const origem = store && typeof store === "object" ? store : {};
    const vazio = !Array.isArray(origem.shifts)
        && !Array.isArray(origem.rotationGroups)
        && !Array.isArray(origem.groupAssignments)
        && !Array.isArray(origem.rotationRules);
    const base = vazio ? criarStoreInicial(tecnicosValidos) : origem;
    const referenceWeek = obterInicioSemana(base.referenceWeek || base.reference_week || SEMANA_REFERENCIA_PADRAO) || SEMANA_REFERENCIA_PADRAO;

    const shifts = (Array.isArray(base.shifts) ? base.shifts : [])
        .map((item, index) => ({
            id: String(item.id || item.shift_id || `turno_${index + 1}`).trim(),
            name: String(item.name || item.nome || `Turno ${index + 1}`).trim().slice(0, 80),
            start_time: normalizarHora(item.start_time || item.startTime || item.entrada, "08:00"),
            end_time: normalizarHora(item.end_time || item.endTime || item.saida, "17:00"),
            sort_order: Number.isInteger(Number(item.sort_order ?? item.sortOrder)) ? Number(item.sort_order ?? item.sortOrder) : index + 1,
            active: normalizarBooleano(item.active, true),
            tipo_turno: normalizarTipoTurno(item.tipo_turno || item.tipoTurno || item.type || item.tipo),
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => item.id && item.name && item.start_time && item.end_time)
        .sort(ordenarPorOrdemENome);

    const rotationGroups = (Array.isArray(base.rotationGroups) ? base.rotationGroups : [])
        .map((item, index) => ({
            id: String(item.id || item.rotation_group_id || `grupo_${index + 1}`).trim(),
            name: String(item.name || item.nome || `Grupo ${index + 1}`).trim().slice(0, 80),
            sort_order: Number.isInteger(Number(item.sort_order ?? item.sortOrder)) ? Number(item.sort_order ?? item.sortOrder) : index + 1,
            active: normalizarBooleano(item.active, true),
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => item.id && item.name)
        .sort(ordenarPorOrdemENome);

    const groupIds = new Set(rotationGroups.map(item => item.id));
    const groupAssignments = (Array.isArray(base.groupAssignments) ? base.groupAssignments : [])
        .map((item, index) => ({
            id: String(item.id || `atribuicao_${index + 1}`).trim(),
            technician_id: String(item.technician_id || item.technicianId || "").trim(),
            rotation_group_id: String(item.rotation_group_id || item.rotationGroupId || "").trim(),
            valid_from_week: obterInicioSemana(item.valid_from_week || item.validFromWeek || SEMANA_REFERENCIA_PADRAO),
            valid_until_week: item.valid_until_week || item.validUntilWeek
                ? obterInicioSemana(item.valid_until_week || item.validUntilWeek)
                : null,
            sort_order: Number.isInteger(Number(item.sort_order ?? item.sortOrder)) ? Number(item.sort_order ?? item.sortOrder) : index + 1,
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => item.id && item.technician_id && groupIds.has(item.rotation_group_id) && item.valid_from_week)
        .sort((a, b) => {
            const tecnico = a.technician_id.localeCompare(b.technician_id, "pt-BR");
            if (tecnico !== 0) return tecnico;
            return a.valid_from_week.localeCompare(b.valid_from_week);
        });

    const shiftIds = new Set(shifts.map(item => item.id));
    const alternanciaShiftIds = new Set(shifts.filter(isTurnoAlternancia).map(item => item.id));
    const rotationRules = (Array.isArray(base.rotationRules) ? base.rotationRules : [])
        .map((item, index) => ({
            id: String(item.id || `regra_${index + 1}`).trim(),
            rotation_group_id: String(item.rotation_group_id || item.rotationGroupId || "").trim(),
            cycle_position: Number.isInteger(Number(item.cycle_position ?? item.cyclePosition)) ? Number(item.cycle_position ?? item.cyclePosition) : 0,
            shift_id: String(item.shift_id || item.shiftId || "").trim(),
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => item.id && groupIds.has(item.rotation_group_id) && alternanciaShiftIds.has(item.shift_id) && item.cycle_position >= 0)
        .sort((a, b) => a.cycle_position - b.cycle_position || a.rotation_group_id.localeCompare(b.rotation_group_id));

    let shiftRotationRules = (Array.isArray(base.shiftRotationRules) ? base.shiftRotationRules : [])
        .map((item, index) => ({
            id: String(item.id || `rotacao_turno_${index + 1}`).trim(),
            shift_id: String(item.shift_id || item.shiftId || item.sourceShiftId || item.source_shift_id || "").trim(),
            next_shift_id: String(item.next_shift_id || item.nextShiftId || item.targetShiftId || item.target_shift_id || "").trim(),
            valid_from_week: obterInicioSemana(item.valid_from_week || item.validFromWeek || referenceWeek),
            valid_until_week: item.valid_until_week || item.validUntilWeek
                ? obterInicioSemana(item.valid_until_week || item.validUntilWeek)
                : null,
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => (
            item.id
            && alternanciaShiftIds.has(item.shift_id)
            && alternanciaShiftIds.has(item.next_shift_id)
            && item.valid_from_week
            && (!item.valid_until_week || item.valid_until_week >= item.valid_from_week)
        ))
        .sort((a, b) => a.shift_id.localeCompare(b.shift_id) || a.valid_from_week.localeCompare(b.valid_from_week));

    if (!shiftRotationRules.length) {
        shiftRotationRules = criarRegrasRotacaoTurnosPadrao(shifts, rotationRules, referenceWeek);
    }

    const weeklyOverrides = (Array.isArray(base.weeklyOverrides) ? base.weeklyOverrides : [])
        .map((item, index) => ({
            id: String(item.id || `excecao_${index + 1}`).trim(),
            week_start: obterInicioSemana(item.week_start || item.weekStart || item.data),
            technician_id: String(item.technician_id || item.technicianId || "").trim(),
            shift_id: String(item.shift_id || item.shiftId || "").trim(),
            rotation_group_id: item.rotation_group_id || item.rotationGroupId
                ? String(item.rotation_group_id || item.rotationGroupId).trim()
                : null,
            sort_order: Number.isInteger(Number(item.sort_order ?? item.sortOrder)) ? Number(item.sort_order ?? item.sortOrder) : index + 1,
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => item.id && item.week_start && item.technician_id && (shiftIds.has(item.shift_id) || isTurnoSemAtribuicao(item.shift_id)))
        .sort((a, b) => a.week_start.localeCompare(b.week_start) || a.technician_id.localeCompare(b.technician_id, "pt-BR"));

    const fixedShiftAssignments = (Array.isArray(base.fixedShiftAssignments) ? base.fixedShiftAssignments : [])
        .map((item, index) => ({
            id: String(item.id || `fixo_${index + 1}`).trim(),
            shift_id: String(item.shift_id || item.shiftId || "").trim(),
            technician_id: String(item.technician_id || item.technicianId || "").trim(),
            sort_order: Number.isInteger(Number(item.sort_order ?? item.sortOrder)) ? Number(item.sort_order ?? item.sortOrder) : index + 1,
            created_at: item.created_at || item.createdAt || new Date().toISOString(),
            updated_at: item.updated_at || item.updatedAt || new Date().toISOString()
        }))
        .filter(item => item.id && item.technician_id && shifts.find(shift => shift.id === item.shift_id && isTurnoFixo(shift)))
        .sort((a, b) => {
            const turno = a.shift_id.localeCompare(b.shift_id);
            if (turno !== 0) return turno;
            const ordem = Number(a.sort_order || 0) - Number(b.sort_order || 0);
            if (ordem !== 0) return ordem;
            return a.technician_id.localeCompare(b.technician_id, "pt-BR");
        });
    const maiorFixedAssignmentId = fixedShiftAssignments.reduce((maior, item) => {
        const numero = Number(String(item.id || "").match(/(\d+)$/)?.[1] || 0);
        return Math.max(maior, numero);
    }, 0);

    return {
        referenceWeek,
        nextIds: {
            shift: Number(base.nextIds?.shift || base.next_ids?.shift || shifts.length + 1),
            group: Number(base.nextIds?.group || base.next_ids?.group || rotationGroups.length + 1),
            assignment: Number(base.nextIds?.assignment || base.next_ids?.assignment || groupAssignments.length + 1),
            rule: Number(base.nextIds?.rule || base.next_ids?.rule || rotationRules.length + 1),
            shiftRotationRule: Number(base.nextIds?.shiftRotationRule || base.next_ids?.shiftRotationRule || base.nextIds?.shift_rotation_rule || base.next_ids?.shift_rotation_rule || shiftRotationRules.length + 1),
            override: Number(base.nextIds?.override || base.next_ids?.override || weeklyOverrides.length + 1),
            fixedAssignment: Number(base.nextIds?.fixedAssignment || base.next_ids?.fixedAssignment || base.nextIds?.fixed_assignment || base.next_ids?.fixed_assignment || maiorFixedAssignmentId + 1)
        },
        shifts,
        rotationGroups,
        groupAssignments,
        rotationRules,
        shiftRotationRules,
        fixedShiftAssignments,
        weeklyOverrides,
        updated_at: base.updated_at || base.updatedAt || new Date().toISOString()
    };
}

function lerStoreLocal(tecnicosValidos = new Set()) {
    garantirArquivoHorariosSemana(tecnicosValidos);
    const conteudo = fs.readFileSync(ARQUIVO_HORARIOS_SEMANA, "utf8");
    return normalizarStore(JSON.parse(conteudo || "{}"), tecnicosValidos);
}

function salvarStoreLocal(store, tecnicosValidos = new Set()) {
    garantirArquivoHorariosSemana(tecnicosValidos);
    const normalizado = normalizarStore(store, tecnicosValidos);
    const arquivoTemporario = `${ARQUIVO_HORARIOS_SEMANA}.tmp`;

    fs.writeFileSync(arquivoTemporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(arquivoTemporario, ARQUIVO_HORARIOS_SEMANA);
    return normalizado;
}

function gerarId(store, tipo, prefixo) {
    const atual = Number(store.nextIds?.[tipo]) || 1;
    store.nextIds[tipo] = atual + 1;
    return `${prefixo}_${atual}`;
}

function gerarIdUnico(store, tipo, prefixo, existentes = new Set()) {
    let id = gerarId(store, tipo, prefixo);

    while (existentes.has(id)) {
        id = gerarId(store, tipo, prefixo);
    }

    existentes.add(id);
    return id;
}

async function garantirSeedMariaDb(conn, tecnicosValidos = new Set()) {
    const rows = await conn.query(`SELECT COUNT(*) AS total FROM ${TABELAS.shifts}`);
    const total = Number(rows?.[0]?.total || 0);
    if (total > 0) return;

    const inicial = criarStoreInicial(tecnicosValidos);
    await salvarStoreMariaDb(conn, inicial);
}

async function carregarStoreMariaDb(conn, tecnicosValidos = new Set()) {
    await garantirSeedMariaDb(conn, tecnicosValidos);

    const [shifts, groups, assignments, rules, shiftRotationRules, overrides, fixedAssignments] = await Promise.all([
        conn.query(`SELECT id, name, start_time, end_time, sort_order, active, tipo_turno, created_at, updated_at FROM ${TABELAS.shifts}`),
        conn.query(`SELECT id, name, sort_order, active, created_at, updated_at FROM ${TABELAS.groups}`),
        conn.query(`SELECT id, technician_id, rotation_group_id, valid_from_week, valid_until_week, sort_order, created_at, updated_at FROM ${TABELAS.assignments}`),
        conn.query(`SELECT id, rotation_group_id, cycle_position, shift_id, created_at, updated_at FROM ${TABELAS.rules}`),
        conn.query(`SELECT id, shift_id, next_shift_id, valid_from_week, valid_until_week, created_at, updated_at FROM ${TABELAS.shiftRotationRules}`),
        conn.query(`SELECT id, week_start, technician_id, shift_id, rotation_group_id, sort_order, created_at, updated_at FROM ${TABELAS.overrides}`),
        conn.query(`SELECT id, shift_id, technician_id, sort_order, created_at, updated_at FROM ${TABELAS.fixedAssignments}`)
    ]);

    return normalizarStore({
        referenceWeek: SEMANA_REFERENCIA_PADRAO,
        shifts: shifts.map(item => ({
            ...item,
            start_time: normalizarHoraSaida(item.start_time),
            end_time: normalizarHoraSaida(item.end_time),
            active: Boolean(item.active),
            tipo_turno: normalizarTipoTurno(item.tipo_turno)
        })),
        rotationGroups: groups.map(item => ({
            ...item,
            active: Boolean(item.active)
        })),
        groupAssignments: assignments.map(item => ({
            ...item,
            valid_from_week: normalizarDataSaida(item.valid_from_week),
            valid_until_week: item.valid_until_week ? normalizarDataSaida(item.valid_until_week) : null
        })),
        rotationRules: rules,
        shiftRotationRules: shiftRotationRules.map(item => ({
            ...item,
            valid_from_week: normalizarDataSaida(item.valid_from_week),
            valid_until_week: item.valid_until_week ? normalizarDataSaida(item.valid_until_week) : null
        })),
        weeklyOverrides: overrides.map(item => ({
            ...item,
            week_start: normalizarDataSaida(item.week_start)
        })),
        fixedShiftAssignments: fixedAssignments
    }, tecnicosValidos);
}

async function salvarStoreMariaDb(conn, storeOrigem) {
    const store = normalizarStore(storeOrigem);

    await conn.query(`DELETE FROM ${TABELAS.fixedAssignments}`);
    await conn.query(`DELETE FROM ${TABELAS.overrides}`);
    await conn.query(`DELETE FROM ${TABELAS.assignments}`);
    await conn.query(`DELETE FROM ${TABELAS.rules}`);
    await conn.query(`DELETE FROM ${TABELAS.shiftRotationRules}`);
    await conn.query(`DELETE FROM ${TABELAS.groups}`);
    await conn.query(`DELETE FROM ${TABELAS.shifts}`);

    for (const shift of store.shifts) {
        await conn.query(`
            INSERT INTO ${TABELAS.shifts}
                (id, name, start_time, end_time, sort_order, active, tipo_turno, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            shift.id,
            shift.name,
            horaComSegundos(shift.start_time),
            horaComSegundos(shift.end_time),
            shift.sort_order,
            shift.active ? 1 : 0,
            shift.tipo_turno,
            shift.created_at,
            shift.updated_at
        ]);
    }

    for (const group of store.rotationGroups) {
        await conn.query(`
            INSERT INTO ${TABELAS.groups}
                (id, name, sort_order, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            group.id,
            group.name,
            group.sort_order,
            group.active ? 1 : 0,
            group.created_at,
            group.updated_at
        ]);
    }

    for (const rule of store.rotationRules) {
        await conn.query(`
            INSERT INTO ${TABELAS.rules}
                (id, rotation_group_id, cycle_position, shift_id, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            rule.id,
            rule.rotation_group_id,
            rule.cycle_position,
            rule.shift_id,
            rule.created_at,
            rule.updated_at
        ]);
    }

    for (const rule of store.shiftRotationRules) {
        await conn.query(`
            INSERT INTO ${TABELAS.shiftRotationRules}
                (id, shift_id, next_shift_id, valid_from_week, valid_until_week, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            rule.id,
            rule.shift_id,
            rule.next_shift_id,
            rule.valid_from_week,
            rule.valid_until_week,
            rule.created_at,
            rule.updated_at
        ]);
    }

    for (const assignment of store.groupAssignments) {
        await conn.query(`
            INSERT INTO ${TABELAS.assignments}
                (id, technician_id, rotation_group_id, valid_from_week, valid_until_week, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            assignment.id,
            assignment.technician_id,
            assignment.rotation_group_id,
            assignment.valid_from_week,
            assignment.valid_until_week,
            assignment.sort_order,
            assignment.created_at,
            assignment.updated_at
        ]);
    }

    for (const override of store.weeklyOverrides) {
        await conn.query(`
            INSERT INTO ${TABELAS.overrides}
                (id, week_start, technician_id, shift_id, rotation_group_id, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            override.id,
            override.week_start,
            override.technician_id,
            override.shift_id,
            override.rotation_group_id,
            override.sort_order,
            override.created_at,
            override.updated_at
        ]);
    }

    for (const assignment of store.fixedShiftAssignments) {
        await conn.query(`
            INSERT INTO ${TABELAS.fixedAssignments}
                (id, shift_id, technician_id, sort_order, created_at, updated_at)
            VALUES (?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP), COALESCE(?, CURRENT_TIMESTAMP))
        `, [
            assignment.id,
            assignment.shift_id,
            assignment.technician_id,
            assignment.sort_order,
            assignment.created_at,
            assignment.updated_at
        ]);
    }

    return store;
}

function obterCycleLength(store) {
    const maxCycle = store.rotationRules.reduce((maior, item) => Math.max(maior, Number(item.cycle_position) || 0), 0);
    return Math.max(1, maxCycle + 1);
}

function diferencaSemanas(weekStart, referenceWeek) {
    const semana = dataParaUtc(weekStart);
    const referencia = dataParaUtc(referenceWeek);
    if (!semana || !referencia) return 0;

    const MS_SEMANA = 7 * 24 * 60 * 60 * 1000;
    return Math.round((semana.getTime() - referencia.getTime()) / MS_SEMANA);
}

function obterCyclePosition(weekStart, store) {
    const cycleLength = obterCycleLength(store);
    const diferenca = diferencaSemanas(weekStart, store.referenceWeek);
    return ((diferenca % cycleLength) + cycleLength) % cycleLength;
}

function obterRegraRotacaoTurnoNaSemana(store, shiftId, weekStartOrigem) {
    const weekStart = obterInicioSemana(weekStartOrigem || obterHojeBrasil());
    if (!weekStart || !shiftId) return null;

    return store.shiftRotationRules
        .filter(rule => (
            rule.shift_id === shiftId
            && rule.valid_from_week <= weekStart
            && (!rule.valid_until_week || rule.valid_until_week >= weekStart)
        ))
        .sort((a, b) => b.valid_from_week.localeCompare(a.valid_from_week))[0] || null;
}

function obterProximoTurnoNaSemana(store, shiftId, weekStart) {
    if (isTurnoSemAtribuicao(shiftId)) return shiftId;
    const turno = store.shifts.find(item => item.id === shiftId);
    if (isTurnoFixo(turno)) return shiftId;
    const regra = obterRegraRotacaoTurnoNaSemana(store, shiftId, weekStart);
    return regra?.next_shift_id || shiftId;
}

function calcularTurnoPorRotacaoTurnos(store, shiftIdOrigem, semanaOrigem, semanaDestino) {
    const inicio = obterInicioSemana(semanaOrigem || store.referenceWeek);
    const destino = obterInicioSemana(semanaDestino || inicio);
    if (!inicio || !destino || !shiftIdOrigem) return shiftIdOrigem || null;
    if (destino <= inicio) return shiftIdOrigem;
    const turnoOrigem = store.shifts.find(item => item.id === shiftIdOrigem);
    if (isTurnoFixo(turnoOrigem)) return shiftIdOrigem;

    const limiteSemanas = Math.max(0, Math.min(520, diferencaSemanas(destino, inicio)));
    let turnoAtual = shiftIdOrigem;
    let semanaCursor = inicio;

    for (let index = 0; index < limiteSemanas; index += 1) {
        turnoAtual = obterProximoTurnoNaSemana(store, turnoAtual, semanaCursor) || turnoAtual;
        semanaCursor = adicionarDiasData(semanaCursor, 7);
    }

    return turnoAtual;
}

function obterTurnoInicialGrupoNaSemana(store, rotationGroupId, weekStart) {
    const cyclePosition = obterCyclePosition(weekStart, store);
    const turnoNaSemana = obterTurnoPorGrupoNaSemana(store, rotationGroupId, cyclePosition);
    if (turnoNaSemana) return turnoNaSemana;

    return store.rotationRules.find(item => item.rotation_group_id === rotationGroupId)?.shift_id || null;
}

function obterTurnoPorAtribuicaoNaSemana(store, assignment, weekStart) {
    const semanaInicioAtribuicao = assignment?.valid_from_week || store.referenceWeek;
    const turnoInicial = obterTurnoInicialGrupoNaSemana(store, assignment?.rotation_group_id, semanaInicioAtribuicao);

    return calcularTurnoPorRotacaoTurnos(store, turnoInicial, semanaInicioAtribuicao, weekStart);
}

function obterTurnoGrupoNaSemana(store, rotationGroupId, weekStart) {
    const turnoInicial = obterTurnoInicialGrupoNaSemana(store, rotationGroupId, store.referenceWeek);
    return calcularTurnoPorRotacaoTurnos(store, turnoInicial, store.referenceWeek, weekStart);
}

function obterMapaDestinosRotacaoTurnos(store, weekStart) {
    const turnosAlternancia = store.shifts.filter(isTurnoAlternancia);
    const shiftIds = new Set(turnosAlternancia.map(item => item.id));
    const mapa = new Map();

    turnosAlternancia.forEach(shift => {
        const nextShiftId = obterProximoTurnoNaSemana(store, shift.id, weekStart);
        mapa.set(shift.id, shiftIds.has(nextShiftId) ? nextShiftId : shift.id);
    });

    return mapa;
}

function obterCycleLengthRotacaoTurnos(store, weekStart) {
    const turnosAtivos = store.shifts.filter(item => item.active !== false && isTurnoAlternancia(item));
    if (!turnosAtivos.length || !store.shiftRotationRules.length) return obterCycleLength(store);

    const destinos = obterMapaDestinosRotacaoTurnos(store, weekStart);
    let maiorCiclo = 1;

    turnosAtivos.forEach(shift => {
        const visitados = new Map();
        let turnoAtual = shift.id;

        for (let index = 0; index < Math.max(1, turnosAtivos.length + 1); index += 1) {
            if (visitados.has(turnoAtual)) {
                maiorCiclo = Math.max(maiorCiclo, index - visitados.get(turnoAtual));
                return;
            }

            visitados.set(turnoAtual, index);
            turnoAtual = destinos.get(turnoAtual) || turnoAtual;
        }
    });

    return maiorCiclo;
}

function coletarAlertasRotacaoTurnos(store, weekStart) {
    const turnosPorId = new Map(store.shifts.map(item => [item.id, item]));
    const alertas = [];

    store.shifts
        .filter(shift => shift.active !== false && isTurnoAlternancia(shift))
        .forEach(shift => {
            const nextShiftId = obterProximoTurnoNaSemana(store, shift.id, weekStart);
            const destino = turnosPorId.get(nextShiftId);
            if (!destino) {
                alertas.push({
                    type: "invalid_shift_rotation",
                    shiftId: shift.id,
                    shiftName: shift.name,
                    nextShiftId,
                    message: `${shift.name} possui rotação configurada para um turno inexistente.`
                });
                return;
            }

            if (destino.active === false) {
                alertas.push({
                    type: "inactive_shift_rotation",
                    shiftId: shift.id,
                    shiftName: shift.name,
                    nextShiftId,
                    nextShiftName: destino.name,
                    message: `${shift.name} possui rotação configurada para ${destino.name}, que está inativo.`
                });
            }
        });

    return alertas;
}

function obterAtribuicoesValidasNaSemana(store, weekStart) {
    const porTecnico = new Map();

    store.groupAssignments.forEach(item => {
        if (item.valid_from_week > weekStart) return;
        if (item.valid_until_week && item.valid_until_week < weekStart) return;

        const atual = porTecnico.get(item.technician_id);
        if (!atual || item.valid_from_week >= atual.valid_from_week) {
            porTecnico.set(item.technician_id, item);
        }
    });

    return porTecnico;
}

function obterTurnoPorGrupoNaSemana(store, rotationGroupId, cyclePosition) {
    return store.rotationRules.find(item => (
        item.rotation_group_id === rotationGroupId
        && Number(item.cycle_position) === Number(cyclePosition)
    ))?.shift_id || null;
}

function obterGrupoAtivoPorId(store, rotationGroupId) {
    return store.rotationGroups.find(item => item.id === rotationGroupId && item.active) || null;
}

function obterGrupoMapeadoParaTurno(store, shiftId, cyclePosition) {
    const regra = store.rotationRules.find(item => (
        item.shift_id === shiftId
        && Number(item.cycle_position) === Number(cyclePosition)
        && obterGrupoAtivoPorId(store, item.rotation_group_id)
    ));

    return regra ? obterGrupoAtivoPorId(store, regra.rotation_group_id) : null;
}

function obterGrupoMapeadoParaTurnoNaSemana(store, shiftId, weekStart) {
    return store.rotationGroups
        .filter(group => group.active)
        .find(group => obterTurnoGrupoNaSemana(store, group.id, weekStart) === shiftId) || null;
}

function obterAtribuicaoTecnicoNaSemana(store, technicianId, weekStart) {
    const atribuicoes = obterAtribuicoesValidasNaSemana(store, weekStart);
    return atribuicoes.get(technicianId) || null;
}

function obterGrupoEstaticoParaTurno(store, shiftId) {
    const cycleLength = obterCycleLength(store);

    return store.rotationGroups
        .filter(group => group.active)
        .find(group => {
            const regras = store.rotationRules.filter(rule => rule.rotation_group_id === group.id);
            if (!regras.length) return false;

            for (let posicao = 0; posicao < cycleLength; posicao += 1) {
                const regra = regras.find(item => Number(item.cycle_position) === posicao);
                if (!regra || regra.shift_id !== shiftId) return false;
            }

            return true;
        }) || null;
}

function garantirGrupoEstaticoParaTurno(store, shiftId) {
    const existente = obterGrupoEstaticoParaTurno(store, shiftId);
    if (existente) return existente;

    const shift = store.shifts.find(item => item.id === shiftId);
    if (!shift) throw new TechnicianScheduleError("Turno não encontrado para criar grupo de rotação.", 404);
    if (isTurnoFixo(shift)) throw new TechnicianScheduleError("Horário fixo não participa de grupos de rotação.", 400);

    const now = new Date().toISOString();
    const idsGrupos = new Set(store.rotationGroups.map(item => item.id));
    const idsRegras = new Set(store.rotationRules.map(item => item.id));
    const groupId = gerarIdUnico(store, "group", "grupo", idsGrupos);
    const maiorOrdem = store.rotationGroups.reduce((maior, item) => Math.max(maior, Number(item.sort_order) || 0), 0);
    const cycleLength = obterCycleLength(store);
    const group = {
        id: groupId,
        name: `Grupo ${shift.name}`,
        sort_order: maiorOrdem + 1,
        active: true,
        created_at: now,
        updated_at: now
    };

    store.rotationGroups.push(group);

    for (let posicao = 0; posicao < cycleLength; posicao += 1) {
        store.rotationRules.push({
            id: gerarIdUnico(store, "rule", "regra", idsRegras),
            rotation_group_id: groupId,
            cycle_position: posicao,
            shift_id: shiftId,
            created_at: now,
            updated_at: now
        });
    }

    return group;
}

function serializarTurnoBase(shift) {
    return {
        id: shift.id,
        name: shift.name,
        startTime: shift.start_time,
        endTime: shift.end_time,
        start_time: shift.start_time,
        end_time: shift.end_time,
        sortOrder: shift.sort_order,
        sort_order: shift.sort_order,
        active: Boolean(shift.active),
        tipoTurno: normalizarTipoTurno(shift.tipo_turno),
        tipo_turno: normalizarTipoTurno(shift.tipo_turno),
        horario: `${shift.start_time} às ${shift.end_time}`,
        nextShiftId: shift.id,
        next_shift_id: shift.id,
        nextShiftName: shift.name,
        next_shift_name: shift.name,
        nextShiftActive: Boolean(shift.active),
        next_shift_active: Boolean(shift.active),
        technicians: [],
        tecnicos: [],
        rotationGroups: []
    };
}

function serializarGrupo(group) {
    return {
        id: group.id,
        name: group.name,
        sortOrder: group.sort_order,
        sort_order: group.sort_order,
        active: Boolean(group.active)
    };
}

function serializarTecnico({ technicianId, assignment, override, fixedAssignment, group, shiftId, active }) {
    const sortOrder = Number(fixedAssignment?.sort_order ?? override?.sort_order ?? assignment?.sort_order ?? 0);
    const source = fixedAssignment ? "fixed" : override ? "override" : "rotation";

    return {
        id: technicianId,
        name: technicianId,
        technicianId,
        technician_id: technicianId,
        rotationGroupId: fixedAssignment ? null : override?.rotation_group_id || assignment?.rotation_group_id || null,
        rotation_group_id: fixedAssignment ? null : override?.rotation_group_id || assignment?.rotation_group_id || null,
        rotationGroupName: group?.name || null,
        rotation_group_name: group?.name || null,
        shiftId,
        shift_id: shiftId,
        source,
        sortOrder,
        sort_order: sortOrder,
        active: Boolean(active)
    };
}

function montarAgendaSemana(store, weekStartOrigem, tecnicosValidos = new Set()) {
    const weekStart = obterInicioSemana(weekStartOrigem || obterHojeBrasil());
    if (!weekStart) {
        throw new TechnicianScheduleError("Informe uma semana válida no formato YYYY-MM-DD.", 400);
    }

    const weekEnd = obterFimSemana(weekStart);
    const cycleLength = obterCycleLengthRotacaoTurnos(store, weekStart);
    const diferencaCiclo = diferencaSemanas(weekStart, store.referenceWeek);
    const cyclePosition = ((diferencaCiclo % cycleLength) + cycleLength) % cycleLength;
    const activeTechnicians = tecnicosValidos instanceof Set ? tecnicosValidos : new Set();
    const shiftsOrdenados = [...store.shifts].sort(ordenarPorOrdemENome);
    const turnosPorId = new Map(shiftsOrdenados.map(shift => [shift.id, shift]));
    const shiftsPorId = new Map(shiftsOrdenados.map(shift => {
        const turno = serializarTurnoBase(shift);
        const nextShiftId = isTurnoFixo(shift) ? shift.id : obterProximoTurnoNaSemana(store, shift.id, weekStart);
        const nextShift = turnosPorId.get(nextShiftId) || shift;

        turno.nextShiftId = nextShift.id;
        turno.next_shift_id = nextShift.id;
        turno.nextShiftName = nextShift.name;
        turno.next_shift_name = nextShift.name;
        turno.nextShiftActive = nextShift.active !== false;
        turno.next_shift_active = nextShift.active !== false;

        return [shift.id, turno];
    }));
    const gruposPorId = new Map(store.rotationGroups.map(group => [group.id, group]));
    const atribuicoes = obterAtribuicoesValidasNaSemana(store, weekStart);
    const overrides = store.weeklyOverrides.filter(item => item.week_start === weekStart);
    const overridesPorTecnico = new Map(overrides.map(item => [item.technician_id, item]));
    const assignedTechnicians = new Set();

    store.fixedShiftAssignments.forEach(fixedAssignment => {
        const shift = turnosPorId.get(fixedAssignment.shift_id);
        if (!isTurnoFixo(shift)) return;
        if (activeTechnicians.size && !activeTechnicians.has(fixedAssignment.technician_id)) return;

        const turno = shiftsPorId.get(fixedAssignment.shift_id);
        if (!turno) return;

        assignedTechnicians.add(fixedAssignment.technician_id);
        turno.technicians.push(serializarTecnico({
            technicianId: fixedAssignment.technician_id,
            assignment: null,
            override: null,
            fixedAssignment,
            group: null,
            shiftId: fixedAssignment.shift_id,
            active: !activeTechnicians.size || activeTechnicians.has(fixedAssignment.technician_id)
        }));
    });

    store.rotationGroups
        .filter(group => group.active)
        .forEach(group => {
            const shiftId = obterTurnoGrupoNaSemana(store, group.id, weekStart);
            const shift = turnosPorId.get(shiftId);
            if (isTurnoFixo(shift)) return;
            const turno = shiftsPorId.get(shiftId);
            if (!turno) return;

            turno.rotationGroups.push(serializarGrupo(group));
        });

    atribuicoes.forEach((assignment, technicianId) => {
        if (assignedTechnicians.has(technicianId)) return;
        const override = overridesPorTecnico.get(technicianId);
        const rotationShiftId = obterTurnoPorAtribuicaoNaSemana(store, assignment, weekStart);
        const shiftId = override?.shift_id || rotationShiftId;
        if (isTurnoSemAtribuicao(shiftId)) return;

        const shift = turnosPorId.get(shiftId);
        if (isTurnoFixo(shift)) return;
        const turno = shiftsPorId.get(shiftId);
        if (!turno) return;

        const group = gruposPorId.get(override?.rotation_group_id || assignment.rotation_group_id);
        assignedTechnicians.add(technicianId);
        turno.technicians.push(serializarTecnico({
            technicianId,
            assignment,
            override,
            group,
            shiftId,
            active: !activeTechnicians.size || activeTechnicians.has(technicianId)
        }));
    });

    overrides.forEach(override => {
        if (assignedTechnicians.has(override.technician_id)) return;
        if (isTurnoSemAtribuicao(override.shift_id)) return;
        if (activeTechnicians.size && !activeTechnicians.has(override.technician_id)) return;

        const shift = turnosPorId.get(override.shift_id);
        if (isTurnoFixo(shift)) return;
        const turno = shiftsPorId.get(override.shift_id);
        if (!turno) return;

        const group = override.rotation_group_id ? gruposPorId.get(override.rotation_group_id) : null;
        assignedTechnicians.add(override.technician_id);
        turno.technicians.push(serializarTecnico({
            technicianId: override.technician_id,
            assignment: null,
            override,
            group,
            shiftId: override.shift_id,
            active: !activeTechnicians.size || activeTechnicians.has(override.technician_id)
        }));
    });

    shiftsPorId.forEach(turno => {
        turno.technicians.sort((a, b) => {
            const ordem = Number(a.sortOrder || 0) - Number(b.sortOrder || 0);
            if (ordem !== 0) return ordem;
            return a.name.localeCompare(b.name, "pt-BR");
        });
        turno.tecnicos = turno.technicians.map(item => item.name);
    });

    const availableTechnicians = [...activeTechnicians]
        .filter(technicianId => !assignedTechnicians.has(technicianId))
        .sort((a, b) => a.localeCompare(b, "pt-BR"))
        .map(technicianId => ({
            id: technicianId,
            name: technicianId,
            technicianId,
            technician_id: technicianId,
            active: true
        }));

    return {
        weekStart,
        weekEnd,
        week_start: weekStart,
        week_end: weekEnd,
        referenceWeek: store.referenceWeek,
        reference_week: store.referenceWeek,
        cyclePosition,
        cycle_position: cyclePosition,
        cycleLength,
        cycle_length: cycleLength,
        rotationWarnings: coletarAlertasRotacaoTurnos(store, weekStart),
        rotation_warnings: coletarAlertasRotacaoTurnos(store, weekStart),
        shifts: [...shiftsPorId.values()],
        rotationGroups: store.rotationGroups.map(serializarGrupo),
        availableTechnicians
    };
}

function obterShiftChanges(payload = {}) {
    const origem = Array.isArray(payload.shiftChanges)
        ? payload.shiftChanges
        : Array.isArray(payload.shifts)
            ? payload.shifts
            : [];

    return origem.map(item => {
        const shiftId = String(item.shiftId || item.shift_id || item.id || "").trim();
        const clientShiftId = String(item.clientShiftId || item.client_shift_id || item.tempId || item.temp_id || "").trim();
        const isNew = normalizarBooleano(item.isNew ?? item.is_new ?? item.novo ?? item.criar, false);
        const hasStartTime = item.startTime !== undefined || item.start_time !== undefined || item.entrada !== undefined;
        const hasEndTime = item.endTime !== undefined || item.end_time !== undefined || item.saida !== undefined;
        const hasNextShiftId = item.nextShiftId !== undefined || item.next_shift_id !== undefined || item.proximoTurnoId !== undefined || item.proximo_turno_id !== undefined;
        const hasTipoTurno = item.tipoTurno !== undefined || item.tipo_turno !== undefined || item.type !== undefined || item.tipo !== undefined;

        return {
            shiftId,
            clientShiftId: clientShiftId || shiftId,
            isNew,
            name: String(item.name || item.nome || "").trim(),
            startTime: hasStartTime ? normalizarHora(item.startTime ?? item.start_time ?? item.entrada) : "",
            endTime: hasEndTime ? normalizarHora(item.endTime ?? item.end_time ?? item.saida) : "",
            hasStartTime,
            hasEndTime,
            nextShiftId: hasNextShiftId ? String(item.nextShiftId ?? item.next_shift_id ?? item.proximoTurnoId ?? item.proximo_turno_id ?? "").trim() : "",
            hasNextShiftId,
            tipoTurno: hasTipoTurno ? normalizarTipoTurnoObrigatorio(item.tipoTurno ?? item.tipo_turno ?? item.type ?? item.tipo) : "",
            hasTipoTurno,
            sortOrder: Number.isInteger(Number(item.sortOrder ?? item.sort_order)) ? Number(item.sortOrder ?? item.sort_order) : null,
            active: item.active !== undefined ? normalizarBooleano(item.active, true) : undefined
        };
    }).filter(item => item.isNew || item.shiftId || item.name || item.hasStartTime || item.hasEndTime || item.hasNextShiftId || item.hasTipoTurno || item.active !== undefined);
}

function obterTechnicianMoves(payload = {}) {
    const origem = Array.isArray(payload.technicianMoves) ? payload.technicianMoves : [];

    return origem.map(item => {
        const removeFromShift = normalizarBooleano(item.removeFromShift ?? item.remove_from_shift ?? item.remove ?? item.remover, false);

        return {
            technicianId: String(item.technicianId || item.technician_id || "").trim(),
            sourceShiftId: String(item.sourceShiftId || item.source_shift_id || "").trim(),
            targetShiftId: removeFromShift
                ? TURNO_SEM_ATRIBUICAO_ID
                : String(item.targetShiftId || item.target_shift_id || item.shiftId || item.shift_id || "").trim(),
            targetGroupId: String(item.targetGroupId || item.target_group_id || item.rotationGroupId || item.rotation_group_id || "").trim(),
            applicationMode: String(item.applicationMode || item.application_mode || "only_this_week").trim(),
            sortOrder: Number.isInteger(Number(item.sortOrder ?? item.sort_order)) ? Number(item.sortOrder ?? item.sort_order) : 0,
            removeFromShift
        };
    }).filter(item => item.technicianId || item.targetShiftId || item.targetGroupId || item.removeFromShift);
}

function validarDadosTurno(change) {
    if (change.name !== undefined && !change.name) {
        throw new TechnicianScheduleError("Informe o nome do turno.", 400);
    }

    if (!change.startTime || !change.endTime) {
        throw new TechnicianScheduleError("Informe um horário válido no formato HH:MM.", 400);
    }

    if (change.startTime === change.endTime) {
        throw new TechnicianScheduleError("A saída não pode ser igual à entrada.", 400);
    }
}

function validarShiftChange(change, store) {
    const shift = store.shifts.find(item => item.id === change.shiftId);
    if (!shift) throw new TechnicianScheduleError("Turno não encontrado.", 404);

    if (change.hasStartTime && !change.startTime) {
        throw new TechnicianScheduleError("Informe um horário válido no formato HH:MM.", 400);
    }

    if (change.hasEndTime && !change.endTime) {
        throw new TechnicianScheduleError("Informe um horário válido no formato HH:MM.", 400);
    }

    validarDadosTurno({
        ...change,
        name: change.name || shift.name,
        startTime: change.hasStartTime ? change.startTime : shift.start_time,
        endTime: change.hasEndTime ? change.endTime : shift.end_time
    });
}

function criarTurno(store, change) {
    validarDadosTurno(change);

    const now = new Date().toISOString();
    const idsExistentes = new Set(store.shifts.map(item => item.id));
    const shiftId = gerarIdUnico(store, "shift", "turno", idsExistentes);
    const maiorOrdem = store.shifts.reduce((maior, item) => Math.max(maior, Number(item.sort_order) || 0), 0);

    store.shifts.push({
        id: shiftId,
        name: change.name,
        start_time: change.startTime,
        end_time: change.endTime,
        sort_order: change.sortOrder ?? maiorOrdem + 1,
        active: change.active !== false,
        tipo_turno: change.tipoTurno || TIPO_TURNO_ALTERNANCIA,
        created_at: now,
        updated_at: now
    });

    return shiftId;
}

function validarMove(move, store, tecnicosValidos = new Set()) {
    const activeTechnicians = tecnicosValidos instanceof Set ? tecnicosValidos : new Set();
    const shift = store.shifts.find(item => item.id === move.targetShiftId && item.active);
    const group = move.targetGroupId
        ? store.rotationGroups.find(item => item.id === move.targetGroupId && item.active)
        : null;

    if (!move.technicianId) {
        throw new TechnicianScheduleError("Informe o técnico.", 400);
    }

    if (activeTechnicians.size && !activeTechnicians.has(move.technicianId)) {
        throw new TechnicianScheduleError("Técnico inativo ou não encontrado para nova atribuição.", 400);
    }

    if (!["only_this_week", "from_this_week", "fixed"].includes(move.applicationMode)) {
        throw new TechnicianScheduleError("Modo de aplicação inválido.", 400);
    }

    if (isTurnoSemAtribuicao(move.targetShiftId)) {
        return;
    }

    if (!shift) {
        throw new TechnicianScheduleError("Informe um turno válido para o técnico.", 400);
    }

    if (isTurnoFixo(shift)) {
        return;
    }

    if (!group && move.applicationMode !== "only_this_week") {
        throw new TechnicianScheduleError("Informe um grupo de rotação válido para o técnico.", 400);
    }
}

function validarSemDuplicidadeAtribuicoes(store) {
    const porTecnicoSemana = new Set();

    store.groupAssignments.forEach(item => {
        const chave = `${item.technician_id}:${item.valid_from_week}`;
        if (porTecnicoSemana.has(chave)) {
            throw new TechnicianScheduleError("Já existe atribuição permanente para este técnico no período informado.", 409);
        }
        porTecnicoSemana.add(chave);
    });
}

function resolverGrupoDestinoMove(store, weekStart, move) {
    if (isTurnoFixo(obterTurnoPorId(store, move.targetShiftId))) {
        return null;
    }

    const groupInformado = obterGrupoAtivoPorId(store, move.targetGroupId);
    if (groupInformado) {
        const turnoGrupoNoCiclo = obterTurnoGrupoNaSemana(store, groupInformado.id, weekStart);
        const grupoEstaticoDoTurno = obterGrupoEstaticoParaTurno(store, move.targetShiftId);

        if (
            turnoGrupoNoCiclo === move.targetShiftId
            || grupoEstaticoDoTurno?.id === groupInformado.id
            || move.applicationMode === "only_this_week"
        ) {
            return groupInformado;
        }
    }

    const groupDoTurnoNoCiclo = obterGrupoMapeadoParaTurnoNaSemana(store, move.targetShiftId, weekStart);
    if (groupDoTurnoNoCiclo) return groupDoTurnoNoCiclo;

    const atribuicaoAtual = obterAtribuicaoTecnicoNaSemana(store, move.technicianId, weekStart);
    const grupoAtual = obterGrupoAtivoPorId(store, atribuicaoAtual?.rotation_group_id);

    if (move.applicationMode === "only_this_week" && grupoAtual) {
        return grupoAtual;
    }

    if (move.applicationMode === "only_this_week") {
        return null;
    }

    return garantirGrupoEstaticoParaTurno(store, move.targetShiftId);
}

function normalizarMovesParaSalvar(store, weekStart, moves, shiftIdMap = new Map()) {
    return moves.map(move => {
        const targetShiftId = shiftIdMap.get(move.targetShiftId) || move.targetShiftId;
        const sourceShiftId = shiftIdMap.get(move.sourceShiftId) || move.sourceShiftId;
        const normalizado = {
            ...move,
            targetShiftId,
            sourceShiftId
        };
        if (isTurnoSemAtribuicao(targetShiftId)) {
            return {
                ...normalizado,
                targetGroupId: "",
                targetGroupName: ""
            };
        }

        if (isTurnoFixo(obterTurnoPorId(store, targetShiftId))) {
            return {
                ...normalizado,
                targetGroupId: "",
                targetGroupName: "Grupo Horário Fixo",
                applicationMode: "fixed"
            };
        }

        const group = resolverGrupoDestinoMove(store, weekStart, normalizado);

        return {
            ...normalizado,
            targetGroupId: group?.id || "",
            targetGroupName: group?.name || ""
        };
    });
}

function aplicarMudancasTurnos(store, shiftChanges) {
    const now = new Date().toISOString();
    const shiftIdMap = new Map();

    shiftChanges.forEach(change => {
        if (change.isNew) {
            const novoShiftId = criarTurno(store, change);
            [change.shiftId, change.clientShiftId]
                .filter(Boolean)
                .forEach(id => shiftIdMap.set(id, novoShiftId));
            return;
        }

        validarShiftChange(change, store);
        const shift = store.shifts.find(item => item.id === change.shiftId);

        shift.name = change.name || shift.name;
        if (change.hasStartTime) shift.start_time = change.startTime;
        if (change.hasEndTime) shift.end_time = change.endTime;
        if (change.sortOrder !== null) shift.sort_order = change.sortOrder;
        if (change.active !== undefined) shift.active = change.active;
        if (change.hasTipoTurno) shift.tipo_turno = change.tipoTurno;
        shift.updated_at = now;
    });

    return shiftIdMap;
}

function removerRegrasRotacaoTurnosFixos(store) {
    const alternanciaShiftIds = new Set(store.shifts.filter(isTurnoAlternancia).map(item => item.id));

    store.rotationRules = store.rotationRules.filter(rule => alternanciaShiftIds.has(rule.shift_id));
    store.shiftRotationRules = store.shiftRotationRules.filter(rule => (
        alternanciaShiftIds.has(rule.shift_id)
        && alternanciaShiftIds.has(rule.next_shift_id)
    ));
}

function obterTurnoPorId(store, shiftId) {
    return store.shifts.find(item => item.id === shiftId) || null;
}

function obterVinculoFixoTecnico(store, technicianId) {
    return store.fixedShiftAssignments.find(item => item.technician_id === technicianId) || null;
}

function removerVinculosFixosTecnico(store, technicianId) {
    store.fixedShiftAssignments = store.fixedShiftAssignments.filter(item => item.technician_id !== technicianId);
}

function removerVinculosFixosTurno(store, shiftId) {
    store.fixedShiftAssignments = store.fixedShiftAssignments.filter(item => item.shift_id !== shiftId);
}

function salvarVinculoFixoTecnico(store, move) {
    const now = new Date().toISOString();
    removerVinculosFixosTecnico(store, move.technicianId);

    store.fixedShiftAssignments.push({
        id: gerarId(store, "fixedAssignment", "fixo"),
        shift_id: move.targetShiftId,
        technician_id: move.technicianId,
        sort_order: move.sortOrder || 0,
        created_at: now,
        updated_at: now
    });
}

function encerrarVinculoRotacaoTecnicoApartirSemana(store, weekStart, technicianId) {
    const diaAnterior = adicionarDiasData(weekStart, -1);

    store.weeklyOverrides = store.weeklyOverrides.filter(item => item.technician_id !== technicianId);

    const atribuicoesTecnico = store.groupAssignments
        .filter(item => item.technician_id === technicianId)
        .sort((a, b) => a.valid_from_week.localeCompare(b.valid_from_week));

    store.groupAssignments = store.groupAssignments.filter(item => !(
        item.technician_id === technicianId
        && item.valid_from_week >= weekStart
    ));

    const atual = atribuicoesTecnico.find(item => (
        item.valid_from_week <= weekStart
        && (!item.valid_until_week || item.valid_until_week >= weekStart)
    ));

    if (atual && atual.valid_from_week < weekStart) {
        const existente = store.groupAssignments.find(item => item.id === atual.id);
        if (existente) {
            existente.valid_until_week = diaAnterior;
            existente.updated_at = new Date().toISOString();
        }
    }

    if (atual && atual.valid_from_week === weekStart) {
        store.groupAssignments = store.groupAssignments.filter(item => item.id !== atual.id);
    }
}

function resolverShiftIdMapeado(shiftId, shiftIdMap = new Map()) {
    return shiftIdMap.get(shiftId) || shiftId;
}

function validarRegrasRotacaoTurnos(store, weekStart) {
    const alertas = coletarAlertasRotacaoTurnos(store, weekStart);
    if (!alertas.length) return;

    throw new TechnicianScheduleError(alertas[0].message || "Revise a rotação semanal dos turnos.", 400);
}

function aplicarRegraRotacaoTurno(store, weekStart, shiftId, nextShiftId) {
    const origem = store.shifts.find(item => item.id === shiftId);
    const destino = store.shifts.find(item => item.id === nextShiftId);
    if (!origem) throw new TechnicianScheduleError("Turno de origem da rotação não encontrado.", 404);
    if (isTurnoFixo(origem) || isTurnoFixo(destino)) {
        throw new TechnicianScheduleError("Horário fixo não participa da rotação semanal.", 400);
    }
    if (!destino || (origem.active !== false && destino.active === false)) {
        throw new TechnicianScheduleError("Destino da rotação semanal inválido ou inativo.", 400);
    }

    const regraAtual = obterRegraRotacaoTurnoNaSemana(store, shiftId, weekStart);
    if (regraAtual?.next_shift_id === nextShiftId) return;

    const now = new Date().toISOString();
    const regrasFuturas = store.shiftRotationRules.filter(rule => (
        rule.shift_id === shiftId
        && rule.valid_from_week >= weekStart
        && rule.id !== regraAtual?.id
    ));
    const idsRemovidos = new Set(regrasFuturas.map(rule => rule.id));
    store.shiftRotationRules = store.shiftRotationRules.filter(rule => !idsRemovidos.has(rule.id));

    if (regraAtual && regraAtual.valid_from_week === weekStart) {
        regraAtual.next_shift_id = nextShiftId;
        regraAtual.valid_until_week = null;
        regraAtual.updated_at = now;
        return;
    }

    if (regraAtual) {
        regraAtual.valid_until_week = adicionarDiasData(weekStart, -7);
        regraAtual.updated_at = now;
    }

    const idsExistentes = new Set(store.shiftRotationRules.map(item => item.id));
    store.shiftRotationRules.push({
        id: gerarIdUnico(store, "shiftRotationRule", "rotacao", idsExistentes),
        shift_id: shiftId,
        next_shift_id: nextShiftId,
        valid_from_week: weekStart,
        valid_until_week: null,
        created_at: now,
        updated_at: now
    });
}

function aplicarMudancasRotacaoTurnos(store, weekStart, shiftChanges, shiftIdMap = new Map()) {
    shiftChanges.forEach(change => {
        const shiftId = resolverShiftIdMapeado(change.shiftId || change.clientShiftId, shiftIdMap);
        if (!shiftId) return;
        if (isTurnoFixo(obterTurnoPorId(store, shiftId))) return;

        const deveConfigurarRotacao = change.hasNextShiftId || change.isNew;
        if (!deveConfigurarRotacao) return;

        const nextShiftIdOriginal = change.hasNextShiftId ? change.nextShiftId : shiftId;
        const nextShiftId = resolverShiftIdMapeado(nextShiftIdOriginal, shiftIdMap);
        if (isTurnoFixo(obterTurnoPorId(store, nextShiftId))) return;
        aplicarRegraRotacaoTurno(store, weekStart, shiftId, nextShiftId || shiftId);
    });

    removerRegrasRotacaoTurnosFixos(store);
    validarRegrasRotacaoTurnos(store, weekStart);
}

function aplicarMoveSomenteSemana(store, weekStart, move) {
    const now = new Date().toISOString();
    const atual = store.weeklyOverrides.find(item => (
        item.week_start === weekStart
        && item.technician_id === move.technicianId
    ));

    if (atual) {
        atual.shift_id = move.targetShiftId;
        atual.rotation_group_id = move.targetGroupId || null;
        atual.sort_order = move.sortOrder || atual.sort_order || 0;
        atual.updated_at = now;
        return;
    }

    store.weeklyOverrides.push({
        id: gerarId(store, "override", "excecao"),
        week_start: weekStart,
        technician_id: move.technicianId,
        shift_id: move.targetShiftId,
        rotation_group_id: move.targetGroupId || null,
        sort_order: move.sortOrder || 0,
        created_at: now,
        updated_at: now
    });
}

function aplicarMovePermanente(store, weekStart, move) {
    const now = new Date().toISOString();
    const diaAnterior = adicionarDiasData(weekStart, -1);

    store.weeklyOverrides = store.weeklyOverrides.filter(item => !(
        item.week_start === weekStart
        && item.technician_id === move.technicianId
    ));

    const atribuicoesTecnico = store.groupAssignments
        .filter(item => item.technician_id === move.technicianId)
        .sort((a, b) => a.valid_from_week.localeCompare(b.valid_from_week));

    store.groupAssignments = store.groupAssignments.filter(item => !(
        item.technician_id === move.technicianId
        && item.valid_from_week >= weekStart
    ));

    const atual = atribuicoesTecnico.find(item => (
        item.valid_from_week <= weekStart
        && (!item.valid_until_week || item.valid_until_week >= weekStart)
    ));

    if (atual && atual.valid_from_week < weekStart) {
        const existente = store.groupAssignments.find(item => item.id === atual.id);
        if (existente) {
            existente.valid_until_week = diaAnterior;
            existente.updated_at = now;
        }
    }

    if (atual && atual.valid_from_week === weekStart) {
        store.groupAssignments = store.groupAssignments.filter(item => item.id !== atual.id);
    }

    if (isTurnoSemAtribuicao(move.targetShiftId)) {
        return;
    }

    store.groupAssignments.push({
        id: atual?.valid_from_week === weekStart ? atual.id : gerarId(store, "assignment", "atribuicao"),
        technician_id: move.technicianId,
        rotation_group_id: move.targetGroupId,
        valid_from_week: weekStart,
        valid_until_week: null,
        sort_order: move.sortOrder || atual?.sort_order || 0,
        created_at: atual?.valid_from_week === weekStart ? atual.created_at : now,
        updated_at: now
    });
}

function isMoveComHorarioFixo(store, move) {
    return isTurnoFixo(obterTurnoPorId(store, move.targetShiftId))
        || isTurnoFixo(obterTurnoPorId(store, move.sourceShiftId))
        || Boolean(obterVinculoFixoTecnico(store, move.technicianId));
}

function aplicarMoveHorarioFixo(store, weekStart, move) {
    removerVinculosFixosTecnico(store, move.technicianId);

    if (isTurnoSemAtribuicao(move.targetShiftId)) {
        encerrarVinculoRotacaoTecnicoApartirSemana(store, weekStart, move.technicianId);
        return;
    }

    const targetShift = obterTurnoPorId(store, move.targetShiftId);
    if (!targetShift) {
        throw new TechnicianScheduleError("Informe um turno válido para o técnico.", 400);
    }

    if (isTurnoFixo(targetShift)) {
        encerrarVinculoRotacaoTecnicoApartirSemana(store, weekStart, move.technicianId);
        salvarVinculoFixoTecnico(store, move);
        return;
    }

    aplicarMovePermanente(store, weekStart, {
        ...move,
        applicationMode: "from_this_week"
    });
}

function aplicarMoves(store, weekStart, moves, tecnicosValidos = new Set()) {
    const vistos = new Set();

    moves.forEach(move => {
        validarMove(move, store, tecnicosValidos);
        if (vistos.has(move.technicianId)) {
            throw new TechnicianScheduleError("O mesmo técnico não pode ser movido mais de uma vez no mesmo salvamento.", 409);
        }
        vistos.add(move.technicianId);

        if (move.applicationMode === "fixed" || isMoveComHorarioFixo(store, move)) {
            aplicarMoveHorarioFixo(store, weekStart, move);
        } else if (move.applicationMode === "from_this_week") {
            aplicarMovePermanente(store, weekStart, move);
        } else {
            aplicarMoveSomenteSemana(store, weekStart, move);
        }
    });

    validarSemDuplicidadeAtribuicoes(store);
}

function validarAgendaCalculada(store, weekStart, tecnicosValidos = new Set()) {
    const agenda = montarAgendaSemana(store, weekStart, tecnicosValidos);
    const vistos = new Set();

    agenda.shifts.forEach(shift => {
        shift.technicians.forEach(technician => {
            if (vistos.has(technician.technicianId)) {
                throw new TechnicianScheduleError("Um técnico não pode aparecer em mais de um turno na mesma semana.", 409);
            }
            vistos.add(technician.technicianId);
        });
    });
}

function aplicarPayloadSalvar(storeOrigem, payload = {}, tecnicosValidos = new Set()) {
    const weekStart = obterInicioSemana(payload.weekStart || payload.week_start || obterHojeBrasil());
    if (!weekStart) {
        throw new TechnicianScheduleError("Informe weekStart no formato YYYY-MM-DD.", 400);
    }

    const store = clonar(storeOrigem);
    const shiftChanges = obterShiftChanges(payload);
    const moves = obterTechnicianMoves(payload);
    const agendaAntesMudancaTipo = shiftChanges.some(change => change.hasTipoTurno)
        ? montarAgendaSemana(store, weekStart, tecnicosValidos)
        : null;
    const tecnicosParaPromoverFixo = new Map();

    shiftChanges.forEach(change => {
        if (!change.hasTipoTurno || change.tipoTurno !== TIPO_TURNO_FIXO || !agendaAntesMudancaTipo) return;

        const shiftId = change.shiftId || change.clientShiftId;
        const shiftAtual = store.shifts.find(item => item.id === shiftId);
        if (!shiftAtual || isTurnoFixo(shiftAtual)) return;

        const turnoAtual = agendaAntesMudancaTipo.shifts.find(item => item.id === shiftId);
        tecnicosParaPromoverFixo.set(shiftId, (turnoAtual?.technicians || []).map((technician, index) => ({
            technicianId: technician.technicianId,
            sortOrder: Number(technician.sortOrder || index + 1)
        })));
    });

    const shiftIdMap = aplicarMudancasTurnos(store, shiftChanges);
    shiftChanges.forEach(change => {
        const shiftId = resolverShiftIdMapeado(change.shiftId || change.clientShiftId, shiftIdMap);
        if (!shiftId || !change.hasTipoTurno) return;

        if (change.tipoTurno === TIPO_TURNO_FIXO) {
            const tecnicos = tecnicosParaPromoverFixo.get(change.shiftId || change.clientShiftId) || [];
            tecnicos.forEach(tecnico => {
                aplicarMoveHorarioFixo(store, weekStart, {
                    technicianId: tecnico.technicianId,
                    targetShiftId: shiftId,
                    targetGroupId: "",
                    applicationMode: "fixed",
                    sortOrder: tecnico.sortOrder
                });
            });
        } else {
            removerVinculosFixosTurno(store, shiftId);
        }
    });
    removerRegrasRotacaoTurnosFixos(store);
    aplicarMudancasRotacaoTurnos(store, weekStart, shiftChanges, shiftIdMap);
    const movesNormalizados = normalizarMovesParaSalvar(store, weekStart, moves, shiftIdMap);
    aplicarMoves(store, weekStart, movesNormalizados, tecnicosValidos);
    validarAgendaCalculada(store, weekStart, tecnicosValidos);

    store.updated_at = new Date().toISOString();
    return { store: normalizarStore(store, tecnicosValidos), weekStart };
}

export async function listarHorariosSemana({ weekStart, tecnicosValidos = new Set() } = {}) {
    const resultadoDb = await executarComMariaDb(async conn => {
        const store = await carregarStoreMariaDb(conn, tecnicosValidos);
        return montarAgendaSemana(store, weekStart || obterHojeBrasil(), tecnicosValidos);
    });

    if (resultadoDb) return resultadoDb.valor;

    const store = lerStoreLocal(tecnicosValidos);
    return montarAgendaSemana(store, weekStart || obterHojeBrasil(), tecnicosValidos);
}

export async function salvarHorariosSemana(payload = {}, { tecnicosValidos = new Set() } = {}) {
    const resultadoDb = await executarComMariaDb(async conn => {
        await conn.beginTransaction();
        try {
            const storeAtual = await carregarStoreMariaDb(conn, tecnicosValidos);
            const { store, weekStart } = aplicarPayloadSalvar(storeAtual, payload, tecnicosValidos);
            await salvarStoreMariaDb(conn, store);
            await conn.commit();
            return montarAgendaSemana(store, weekStart, tecnicosValidos);
        } catch (err) {
            await conn.rollback();
            throw err;
        }
    });

    if (resultadoDb) return resultadoDb.valor;

    const storeAtual = lerStoreLocal(tecnicosValidos);
    const { store, weekStart } = aplicarPayloadSalvar(storeAtual, payload, tecnicosValidos);
    salvarStoreLocal(store, tecnicosValidos);
    return montarAgendaSemana(store, weekStart, tecnicosValidos);
}

export async function restaurarConfiguracaoSemana(weekStartOrigem, { tecnicosValidos = new Set() } = {}) {
    const weekStart = obterInicioSemana(weekStartOrigem || obterHojeBrasil());
    if (!weekStart) {
        throw new TechnicianScheduleError("Informe uma semana válida no formato YYYY-MM-DD.", 400);
    }

    const removerOverrides = storeOrigem => {
        const store = clonar(storeOrigem);
        store.weeklyOverrides = store.weeklyOverrides.filter(item => item.week_start !== weekStart);
        store.updated_at = new Date().toISOString();
        return normalizarStore(store, tecnicosValidos);
    };

    const resultadoDb = await executarComMariaDb(async conn => {
        await conn.beginTransaction();
        try {
            const storeAtual = await carregarStoreMariaDb(conn, tecnicosValidos);
            const store = removerOverrides(storeAtual);
            await salvarStoreMariaDb(conn, store);
            await conn.commit();
            return montarAgendaSemana(store, weekStart, tecnicosValidos);
        } catch (err) {
            await conn.rollback();
            throw err;
        }
    });

    if (resultadoDb) return resultadoDb.valor;

    const storeAtual = lerStoreLocal(tecnicosValidos);
    const store = removerOverrides(storeAtual);
    salvarStoreLocal(store, tecnicosValidos);
    return montarAgendaSemana(store, weekStart, tecnicosValidos);
}

export async function listarTurnosLegado({ tecnicosValidos = new Set() } = {}) {
    const semana = await listarHorariosSemana({ weekStart: obterHojeBrasil(), tecnicosValidos });

    return semana.shifts.filter(shift => shift.active !== false).map(shift => ({
        id: shift.id,
        entrada: shift.startTime,
        saida: shift.endTime,
        nome: shift.name
    }));
}

export async function atualizarTurnoLegado(horarioId, payload = {}, { tecnicosValidos = new Set() } = {}) {
    const semana = await listarHorariosSemana({ weekStart: obterHojeBrasil(), tecnicosValidos });
    const turnosAtivos = semana.shifts.filter(item => item.active !== false);
    const legacyMap = new Map([
        ["horario_08", turnosAtivos[0]?.id],
        ["horario_09", turnosAtivos[1]?.id]
    ]);
    const shiftId = legacyMap.get(String(horarioId || "")) || String(horarioId || "");
    const shift = semana.shifts.find(item => item.id === shiftId);

    if (!shift) {
        throw new TechnicianScheduleError("Horário não encontrado.", 404);
    }

    const agenda = await salvarHorariosSemana({
        weekStart: semana.weekStart,
        shiftChanges: [{
            shiftId,
            name: shift.name,
            startTime: payload.entrada || payload.startTime || shift.startTime,
            endTime: payload.saida || payload.endTime || shift.endTime,
            sortOrder: shift.sortOrder,
            active: shift.active
        }]
    }, { tecnicosValidos });
    const atualizado = agenda.shifts.find(item => item.id === shiftId);

    return {
        horario: {
            id: atualizado.id,
            entrada: atualizado.startTime,
            saida: atualizado.endTime,
            nome: atualizado.name
        },
        horarios: agenda.shifts.filter(item => item.active !== false).map(item => ({
            id: item.id,
            entrada: item.startTime,
            saida: item.endTime,
            nome: item.name
        }))
    };
}

export async function obterHorariosEfetivosPorTecnicoData(dataReferencia, tecnicosValidos = new Set()) {
    const agenda = await listarHorariosSemana({
        weekStart: obterInicioSemana(dataReferencia || obterHojeBrasil()),
        tecnicosValidos
    });
    const porTecnico = new Map();

    agenda.shifts.forEach(shift => {
        if (shift.active === false) return;

        shift.technicians.forEach(technician => {
            porTecnico.set(technician.technicianId, {
                technicianId: technician.technicianId,
                shiftId: shift.id,
                shiftName: shift.name,
                startTime: shift.startTime,
                endTime: shift.endTime,
                rotationGroupId: technician.rotationGroupId,
                rotationGroupName: technician.rotationGroupName,
                source: technician.source,
                weekStart: agenda.weekStart,
                weekEnd: agenda.weekEnd,
                cyclePosition: agenda.cyclePosition
            });
        });
    });

    return porTecnico;
}
