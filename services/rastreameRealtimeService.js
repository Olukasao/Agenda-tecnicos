import {
    configurarTecnicosAgendaRastreame,
    obterPosicoesAtualizadasRastreame,
    sincronizarPosicoesRastreame
} from "./rastreameService.js";

const INTERVALO_PADRAO_SEGUNDOS = 60;
const INTERVALO_MINIMO_SEGUNDOS = 5;
const GAP_MINIMO_SYNC_MS = 4000;

let ioServer = null;
let timerRastreameRealtime = null;
let tecnicosRealtime = [];
let isSyncingRastreame = false;
let lastSyncRastreameAt = null;
let lastSyncRastreameError = null;

function obterIntervaloMs() {
    const segundos = Number(process.env.RASTREAME_SYNC_INTERVAL_SECONDS) || INTERVALO_PADRAO_SEGUNDOS;
    return Math.max(INTERVALO_MINIMO_SEGUNDOS, segundos) * 1000;
}

function normalizarTecnicos(tecnicos = []) {
    return [...new Set((Array.isArray(tecnicos) ? tecnicos : [])
        .map(tecnico => String(tecnico || "").trim())
        .filter(Boolean))];
}

function obterStatusRastreame(fontes = []) {
    if (isSyncingRastreame) return "sincronizando";
    if (lastSyncRastreameError) return "erro";
    if (!fontes.length) return "sem_dados";

    const comErro = fontes.filter(fonte => fonte.erro).length;
    if (comErro === 0) return "online";
    if (comErro === fontes.length) return "erro";
    return "parcial";
}

function montarPayloadTempoReal() {
    const dados = obterPosicoesAtualizadasRastreame({ tecnicos: tecnicosRealtime });
    const fontes = dados.fontes || [];
    const tecnicos = (dados.tecnicos || []).map(tecnico => ({
        id: tecnico.tecnicoId,
        nome: tecnico.tecnicoNome || tecnico.tecnicoId,
        tecnicoId: tecnico.tecnicoId,
        tecnicoNome: tecnico.tecnicoNome || tecnico.tecnicoId,
        latitude: tecnico.latitude,
        longitude: tecnico.longitude,
        fonteLocalizacao: "gps_rastreame",
        empresaRastreame: tecnico.empresaRastreame,
        statusPosicaoGps: tecnico.statusPosicaoGps,
        ultimaAtualizacaoLocalizacao: tecnico.ultimaAtualizacaoLocalizacao,
        vinculoRastreameOrigem: tecnico.vinculoRastreameOrigem,
        veiculo: tecnico.veiculo
    }));

    return {
        lastSyncAt: dados.sincronizadoEm || lastSyncRastreameAt,
        sincronizadoEm: dados.sincronizadoEm || lastSyncRastreameAt,
        sincronizando: isSyncingRastreame || Boolean(dados.sincronizando),
        erro: lastSyncRastreameError || dados.erro || null,
        realtimeStatus: obterStatusRastreame(fontes),
        fontes,
        resumo: dados.resumo || {},
        tecnicos,
        veiculos: dados.veiculos || [],
        veiculosSemVinculo: (dados.veiculos || []).filter(veiculo => !veiculo.tecnicoId)
    };
}

export function getRastreameRealtimeStatus() {
    const snapshot = montarPayloadTempoReal();
    return {
        intervaloMs: obterIntervaloMs(),
        isSyncingRastreame,
        lastSyncRastreameAt,
        lastSyncRastreameError,
        realtimeStatus: snapshot.realtimeStatus,
        fontes: snapshot.fontes
    };
}

export function obterRastreameRealtimeSnapshot({ tecnicos = null } = {}) {
    if (tecnicos) {
        tecnicosRealtime = normalizarTecnicos(tecnicos);
        configurarTecnicosAgendaRastreame(tecnicosRealtime);
    }

    return montarPayloadTempoReal();
}

export function emitirAtualizacaoParaClientes(payload = null) {
    const dados = payload || montarPayloadTempoReal();
    if (ioServer) {
        ioServer.emit("rastreame:posicoes_atualizadas", dados);
    }
    return dados;
}

export async function sincronizarRastreameAgora({ force = true, tecnicos = null } = {}) {
    if (tecnicos) {
        tecnicosRealtime = normalizarTecnicos(tecnicos);
        configurarTecnicosAgendaRastreame(tecnicosRealtime);
    }

    if (isSyncingRastreame) {
        return montarPayloadTempoReal();
    }

    const agoraMs = Date.now();
    if (!force && lastSyncRastreameAt && (agoraMs - Date.parse(lastSyncRastreameAt)) < GAP_MINIMO_SYNC_MS) {
        return montarPayloadTempoReal();
    }

    isSyncingRastreame = true;
    emitirAtualizacaoParaClientes();

    try {
        const resultado = await sincronizarPosicoesRastreame({ force: true, tecnicos: tecnicosRealtime });
        lastSyncRastreameAt = resultado.sincronizadoEm || new Date().toISOString();
        lastSyncRastreameError = null;
        isSyncingRastreame = false;
        return emitirAtualizacaoParaClientes();
    } catch (err) {
        lastSyncRastreameError = err.message || "Erro ao sincronizar RastreaMe.";
        isSyncingRastreame = false;
        return emitirAtualizacaoParaClientes();
    }
}

export function sincronizarRastreameSeNecessario({ tecnicos = null } = {}) {
    if (tecnicos) {
        tecnicosRealtime = normalizarTecnicos(tecnicos);
        configurarTecnicosAgendaRastreame(tecnicosRealtime);
    }

    if (isSyncingRastreame) return false;

    const intervaloMs = obterIntervaloMs();
    const ultima = lastSyncRastreameAt ? Date.parse(lastSyncRastreameAt) : 0;
    if (ultima && (Date.now() - ultima) < intervaloMs) return false;

    sincronizarRastreameAgora({ force: true }).catch(err => {
        lastSyncRastreameError = err.message || "Erro ao sincronizar RastreaMe.";
        emitirAtualizacaoParaClientes();
    });
    return true;
}

export function iniciarRastreameRealtime({ io, tecnicos = [] } = {}) {
    if (io) ioServer = io;
    tecnicosRealtime = normalizarTecnicos(tecnicos);
    configurarTecnicosAgendaRastreame(tecnicosRealtime);

    if (timerRastreameRealtime) return getRastreameRealtimeStatus();

    const intervaloMs = obterIntervaloMs();
    timerRastreameRealtime = setInterval(() => {
        sincronizarRastreameAgora({ force: true }).catch(err => {
            lastSyncRastreameError = err.message || "Erro ao sincronizar RastreaMe.";
            emitirAtualizacaoParaClientes();
        });
    }, intervaloMs);

    setTimeout(() => {
        sincronizarRastreameAgora({ force: true }).catch(err => {
            lastSyncRastreameError = err.message || "Erro ao sincronizar RastreaMe.";
            emitirAtualizacaoParaClientes();
        });
    }, 500);

    return getRastreameRealtimeStatus();
}

export function pararRastreameRealtime() {
    if (!timerRastreameRealtime) return false;
    clearInterval(timerRastreameRealtime);
    timerRastreameRealtime = null;
    return true;
}
