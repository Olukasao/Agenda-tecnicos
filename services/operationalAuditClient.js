import fetch from "node-fetch";

const AUDIT_URL = process.env.OPERATIONAL_AUDIT_URL
    || "http://127.0.0.1:8600/api/controle-operacional/ingest";
const AUDIT_TOKEN = process.env.OPERATIONAL_AUDIT_TOKEN
    || process.env.AUDIT_INGEST_TOKEN
    || process.env.JWT_SECRET
    || "chat_secret";
const AUDIT_ENABLED = String(process.env.OPERATIONAL_AUDIT_ENABLED || "false").toLowerCase() === "true";
const AUDIT_TIMEOUT_MS = Number(process.env.OPERATIONAL_AUDIT_TIMEOUT_MS || 2500);

function obterUsuarioReq(req = null) {
    const usuarioId = req?.headers?.["x-user-id"] || req?.headers?.["x-usuario-id"] || null;
    const usuarioNome = req?.headers?.["x-user-name"] || req?.headers?.["x-usuario"] || req?.headers?.["x-user"] || null;

    return {
        usuario_id: Number.isInteger(Number(usuarioId)) ? Number(usuarioId) : null,
        usuario_nome: usuarioNome || (!Number.isInteger(Number(usuarioId)) ? usuarioId : null),
        usuario_perfil: req?.headers?.["x-user-perfil"] || req?.headers?.["x-user-profile"] || null
    };
}

export function registrarAuditoriaOperacional(payload = {}, req = null) {
    if (!AUDIT_ENABLED || !payload.acao || !payload.entidade) return Promise.resolve(false);

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), AUDIT_TIMEOUT_MS);

    const body = {
        origem: "AGENDA_TECNICOS",
        ...obterUsuarioReq(req),
        ...payload,
        metadata: {
            ...(payload.metadata || {}),
            audit_source: "agenda-tecnicos",
            route: req?.originalUrl || req?.url || null
        }
    };

    return fetch(AUDIT_URL, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "x-audit-token": AUDIT_TOKEN
        },
        body: JSON.stringify(body),
        signal: controller.signal
    }).then((response) => {
        clearTimeout(timer);
        if (!response.ok) {
            console.warn("Auditoria operacional não registrada:", response.status);
            return false;
        }
        return true;
    }).catch((err) => {
        clearTimeout(timer);
        console.warn("Auditoria operacional indisponível:", err.message || err);
        return false;
    });
}
