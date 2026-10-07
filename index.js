import express from "express";
import https from "https";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { Server } from "socket.io";
import fetch from "node-fetch";
import FormData from "form-data";
import dotenv from "dotenv";
import {
    configurarTecnicosAgendaRastreame,
    debugCamposRastreame,
    desvincularVeiculoTecnico,
    iniciarSincronizacaoRastreameSeNecessario,
    obterMapaPosicoesTecnicosRastreame,
    obterPosicoesAtualizadasRastreame,
    obterVeiculosRastreame,
    sincronizarPosicoesRastreame,
    testarFontesRastreame,
    vincularVeiculoTecnico
} from "./services/rastreameService.js";
import {
    getRastreameRealtimeStatus,
    iniciarRastreameRealtime,
    obterRastreameRealtimeSnapshot,
    pararRastreameRealtime,
    sincronizarRastreameAgora,
    sincronizarRastreameSeNecessario
} from "./services/rastreameRealtimeService.js";
import {
    AGENDA_LUNCH_PERMISSION,
    AgendaLunchError,
    atualizarAlmocoAgenda,
    criarAlmocoAgenda,
    listarAlmocosAgenda,
    removerAlmocoAgenda
} from "./services/agendaLunchService.js";
import {
    TechnicianScheduleError,
    atualizarTurnoLegado,
    listarHorariosSemana,
    listarTurnosLegado,
    normalizarDataAgenda,
    normalizarHora as normalizarHoraAgendaTecnico,
    obterHorariosEfetivosPorTecnicoData,
    restaurarConfiguracaoSemana,
    salvarHorariosSemana
} from "./services/technicianScheduleService.js";
import {
    MAPA_PONTOS_FIXOS_PERMISSION,
    MapaPontosFixosError,
    atualizarPontoFixoMapa,
    criarPontoFixoMapa,
    listarPontosFixosMapa,
    obterTiposPontosFixos,
    obterVisibilidadesPontosFixos,
    removerPontoFixoMapa
} from "./services/mapaPontosFixosService.js";
import {
    MAPA_DESENHOS_PERMISSION,
    MapaDesenhosError,
    atualizarDesenhoMapa,
    criarDesenhoMapa,
    limparDesenhosDiaMapa,
    listarDesenhosMapa,
    obterFerramentasDesenhoMapa,
    obterTiposAreaDesenhoMapa,
    obterVisibilidadesDesenhoMapa,
    removerDesenhoMapa,
    salvarDesenhoDefinitivoMapa
} from "./services/mapaDesenhosService.js";
import { registrarAuditoriaOperacional } from "./services/operationalAuditClient.js";
import {
    calcularAddressHash,
    obterGeocodeCache,
    salvarGeocodeCache
} from "./services/mapaChamadosGeocodeService.js";

const app = express();
app.use(express.json({ limit: "256kb" }));

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PUBLIC_DIR = path.join(__dirname, "public");
const INDEX_HTML_FILE = path.join(PUBLIC_DIR, "index.html");

dotenv.config({ path: path.join(__dirname, ".env") });



const options = {
    key: fs.readFileSync(path.join(__dirname, "key.pem")),
    cert: fs.readFileSync(path.join(__dirname, "cert.pem"))  // certificado público
};

const PORT = 8500;
const server = https.createServer(options, app);


const io = new Server(server, {
    cors: { origin: "*" }
});


const username = "carloslacerda";
const password = "Bianca@2026";
const basicAuth = Buffer.from(`${username}:${password}`).toString("base64");
const SGP_BASE_URL = "https://acessanet.sgp.net.br";
const ORDEM_SERVICO_TIMEOUT_MS = 20000;
const ORDEM_SERVICO_STATUS_CACHE_TTL_MS = Number(process.env.ORDEM_SERVICO_STATUS_CACHE_TTL_MS || 10 * 1000);
const ORDEM_SERVICO_429_MAX_TENTATIVAS = 2;
const ORDEM_SERVICO_429_BACKOFF_MS = 400;
const ORDEM_SERVICO_INTERVALO_ENTRE_STATUS_MS = 250;
const CONTRATO_TIMEOUT_MS = 8000;
const CONTRATO_CACHE_TTL_MS = 30 * 60 * 1000;
const CONTRATO_ERRO_CACHE_TTL_MS = 60 * 1000;
const CONTRATO_CONCORRENCIA = 12;
const AGENDA_CACHE_TTL_MS = Number(process.env.AGENDA_CACHE_TTL_MS || 10 * 1000);
const AGENDA_STALE_CACHE_TTL_MS = Number(process.env.AGENDA_STALE_CACHE_TTL_MS || 10 * 60 * 1000);
const AGENDA_ENRIQUECIDA_CACHE_TTL_MS = Number(process.env.AGENDA_ENRIQUECIDA_CACHE_TTL_MS || 5 * 60 * 1000);
const OCORRENCIA_SGP_API_URL = `${SGP_BASE_URL}/api/ura/ocorrencia/list/`;
const OCORRENCIA_SGP_TOKEN = "5deb42feb9c2418ab9550c25de640c60";
const OCORRENCIA_SGP_APP = "noc";
const SGP_CHAMADO_UPDATE_TOKEN = process.env.SGP_CHAMADO_UPDATE_TOKEN || process.env.SGP_API_TOKEN || "";
const SGP_CHAMADO_UPDATE_APP = process.env.SGP_CHAMADO_UPDATE_APP || process.env.SGP_API_APP || "";
const SGP_CHAMADO_UPDATE_TIMEOUT_MS = 20000;
const OCORRENCIA_SGP_LIMIT = 500;
const OCORRENCIA_TIMEOUT_MS = 60000;
const MASSIVOS_HISTORICO_CACHE_TTL_MS = 1 * 60 * 1000;
const MASSIVOS_HISTORICO_DIAS_PADRAO = 7;
const MASSIVOS_HISTORICO_DIAS_MAXIMO = 31;
const STATUS_OCORRENCIAS_HISTORICO_MASSIVO = ["0", "1", "2", "3"];
const STATUS_OCORRENCIAS_ATIVAS_MASSIVO = ["0", "2", "3"];
const TIPOS_MASSIVO_SGP = [
    "BACKBONE GPON ACESSANET | MRT",
    "BACKBONE GPON ACESSANET | DAS",
    "BACKBONE GPON ACESSANET | SRP",
    "BACKBONE GPON ACESSANET | PV",
    "BACKBONE GPON ACESSANET | LRJ"
];
const STATUS_ORDEM_SERVICO = ["0", "1", "2"];
const contratoCache = new Map();
const contratoRequests = new Map();
const ordemServicoStatusCache = new Map();
const ordemServicoStatusRequests = new Map();
const agendaCache = new Map();
const agendaRequests = new Map();
const agendaEnriquecidaCache = new Map();
const agendaEnriquecidaRequests = new Map();
const agendaUltimaValidaCache = new Map();
const massivosHistoricoCache = new Map();
const impactoBairrosSemanaCache = new Map();
const WEEKLY_NEIGHBORHOOD_CACHE_MINUTES = 5;
const AGENDA_DEBUG_ENABLED = String(process.env.AGENDA_DEBUG || "").toLowerCase() === "true";

function debugAgenda(...args) {
    if (AGENDA_DEBUG_ENABLED) {
        console.log(...args);
    }
}

// 🔥 técnicos
const MAPA_TECNICOS = {

    "alansantos": "Alan",
    "AlanSantos": "Alan",
    "Alan Santos": "Alan",
    "alan santos": "Alan",
    "alan": "Alan",
    "alexsilva": "Alex",
    "Claudinei": "Claudinei",
    "Emerson Barbosa dos Santos Alves": "Emerson",
    "eniosilva": "Énio",
    "Enio Evangelista da Silva": "Énio",
    "Enio Silva": "Énio",
    "josimarsampaio": "Jó",
    "Leonardosilva": "Leonardo",
    "LUCIO": "Lucio",
    "Nelson": "Nelson",
    "Noc": "Noc",
    "paulinosantos": "Paulino",
    "rafaeloliveira": "Rafael",
    "ramonalmeida": "Ramon",
    "weslleyoliveira": "Weslley",
    "washingtonborges": "Washington",
};

const MAPA_TECNICOS_INSTALACAO_SEM_GPS = {
    "kesiooliveira": "Kesio",
    "jefersondias": "Jeferson Dias"
};

const STATUS_USUARIO_SGP_NAO_VINCULADO = "Usuário SGP ainda não vinculado";
const TECNICOS_INTERNOS_SEM_USUARIO_SGP = [];

function normalizarIdentificadorSgpAgenda(valor) {
    return String(valor || "")
        .trim()
        .toLowerCase();
}

function criarMapaTecnicosNormalizado(mapaTecnicos = {}) {
    return new Map(Object.entries(mapaTecnicos)
        .map(([identificador, tecnico]) => [normalizarIdentificadorSgpAgenda(identificador), tecnico])
        .filter(([identificador, tecnico]) => identificador && tecnico));
}

const MAPA_TECNICOS_NORMALIZADO = criarMapaTecnicosNormalizado(MAPA_TECNICOS);
const MAPA_TECNICOS_INSTALACAO_SEM_GPS_NORMALIZADO = criarMapaTecnicosNormalizado(MAPA_TECNICOS_INSTALACAO_SEM_GPS);

const TECNICOS_SEM_RASTREAMENTO_GPS_TEMPORARIO = [];
const TECNICOS_SEM_RASTREAMENTO_GPS = new Set([
    ...Object.values(MAPA_TECNICOS_INSTALACAO_SEM_GPS),
    ...TECNICOS_SEM_RASTREAMENTO_GPS_TEMPORARIO
]);
const TECNICOS_COORDENADOR_ROTA_INSTALACAO = ["Jeferson Dias", "Kesio", "Leonardo", "Énio"];
const TECNICOS_OCULTOS_AGENDA_TECNICA = new Set([
    "Claudinei",
    "Késio",
    "Kesio",
    "kesiooliveira",
    "Jeferson Dias",
    "jefersondias"
].map(normalizarChaveTecnicoAgendaTecnica));
const TECNICOS_COORDENADOR_ROTA_INSTALACAO_ALIASES = new Map([
    ["jefersondias", "Jeferson Dias"],
    ["jeferson", "Jeferson Dias"],
    ["kesiooliveira", "Kesio"],
    ["kesio", "Kesio"],
    ["kesioo", "Kesio"],
    ["késio", "Kesio"],
    ["leonardosilva", "Leonardo"],
    ["leonardo", "Leonardo"],
    ["eniosilva", "Énio"],
    ["enio", "Énio"],
    ["ênio", "Énio"]
].map(([alias, tecnico]) => [normalizarChaveTecnicoRota(alias), tecnico]));


let plantao = {
    manutencao: [],
    rede: [],
    suporte: []
};

const AREAS = Object.keys(plantao);
const RESPONSAVEIS_SERVICOS_OPCOES = ["Rafael", "Ed", "Alexandre", "Pedro", "Diego", "Carlinhos"];
const RESPONSAVEIS_SERVICOS_FILE = path.join(__dirname, "data", "tecnico_responsaveis_servicos.json");
const CHAT_INTERNO_BASE_URL = (process.env.CHAT_INTERNO_BASE_URL || "http://192.168.1.133:8600").replace(/\/+$/, "");
const CHAT_INTERNO_JWT_SECRET = process.env.CHAT_INTERNO_JWT_SECRET || process.env.JWT_SECRET || "chat_secret";
const CHAT_INTERNO_SERVICE_USER_ID = Number(process.env.CHAT_INTERNO_SERVICE_USER_ID || 3);
const CHAT_INTERNO_EMPRESA_ID = Number(process.env.CHAT_INTERNO_EMPRESA_ID || 1);
const CHAT_INTERNO_TIMEOUT_MS = Number(process.env.CHAT_INTERNO_TIMEOUT_MS || 5000);
const CHAT_INTERNO_USUARIOS_CACHE_TTL_MS = Number(process.env.CHAT_INTERNO_USUARIOS_CACHE_TTL_MS || 5 * 60 * 1000);
let chatInternoUsuariosCache = {
    carregadoEm: 0,
    usuarios: []
};
let chatInternoServiceUserIdCache = Number.isFinite(CHAT_INTERNO_SERVICE_USER_ID)
    ? CHAT_INTERNO_SERVICE_USER_ID
    : 0;
const AUSENCIAS_TECNICOS_FILE = path.join(__dirname, "data", "tecnico_ausencias.json");
const MOTIVO_AUSENCIA_DUPLADO = "duplado";
const MOTIVOS_AUSENCIA_TECNICO = new Set(["medico", "ferias", "atestado", "folga", "mecanico", MOTIVO_AUSENCIA_DUPLADO, "outro"]);
const TAMANHO_MAXIMO_OBSERVACAO_AUSENCIA = 80;
const CORES_EQUIPE_TECNICOS_FILE = path.join(__dirname, "data", "technician_team_colors.json");
const CORES_EQUIPE_TECNICO = new Set(["azul", "verde", "roxo", "laranja", "vermelho", "cinza"]);
const COORDENADOR_ROTA_SUGESTOES_FILE = path.join(__dirname, "data", "agenda_rota_sugestoes.json");
const COORDENADOR_ROTA_LOG_FILE = path.join(__dirname, "data", "agenda_rota_logs.json");
const TECNICOS_LOCALIZACAO_FILE = path.join(__dirname, "data", "tecnicos_localizacao.json");
const CHAMADOS_LOCALIZACAO_FILE = path.join(__dirname, "data", "agenda_chamados_localizacao.json");
const AGENDA_CHAMADOS_SIMULTANEOS_FILE = path.join(__dirname, "data", "agenda_chamados_simultaneos.json");
const MOTIVOS_CRITICOS_ROTA = ["procon", "anatel", "reclame aqui"];
const VELOCIDADE_MEDIA_ROTA_KMH = 28;
const MINUTOS_ATENDIMENTO_ROTA = 12;
const MINUTOS_ATENDIMENTO_SOBRECARGA_PADRAO = 45;
const MINUTOS_PENALIDADE_SEM_LOCALIZACAO_SOBRECARGA = 20;
const MINUTOS_PENALIDADE_CRITICO_SOBRECARGA = 15;
const TURNO_PADRAO_SOBRECARGA = {
    startTime: "08:00",
    endTime: "18:00",
    shiftName: "Horário padrão"
};
const STATUS_LOCALIZACAO_ROTA = new Set([
    "confirmada",
    "corrigida_manual",
    "nao_confirmada",
    "suspeita",
    "sem_localizacao",
    "erro_geocoding"
]);
const FONTES_LOCALIZACAO_ROTA = new Set([
    "sgp",
    "manual",
    "geocoding",
    "corrigida_manual",
    "confirmada",
    "desconhecida"
]);
const CAMPOS_SGP_ENDERECO_COMPLETO = [
    "endereco_completo",
    "enderecoCompleto",
    "endereco",
    "endereço",
    "cliente_endereco",
    "clienteEndereco",
    "logradouro_completo",
    "logradouroCompleto"
];
const CAMPOS_SGP_LOGRADOURO = [
    "endereco_logradouro",
    "enderecoLogradouro",
    "logradouro",
    "rua",
    "nomeRua",
    "nome_rua",
    "cliente_logradouro",
    "clienteLogradouro"
];
const CAMPOS_SGP_NUMERO = [
    "endereco_numero",
    "enderecoNumero",
    "numero",
    "número",
    "numero_endereco",
    "cliente_numero",
    "clienteNumero"
];
const CAMPOS_SGP_BAIRRO = [
    "endereco_bairro",
    "enderecoBairro",
    "bairro",
    "cliente_bairro",
    "clienteBairro",
    "contrato_bairro",
    "contratoBairro"
];
const CAMPOS_SGP_CIDADE = [
    "endereco_cidade",
    "enderecoCidade",
    "cidade",
    "cliente_cidade",
    "clienteCidade",
    "municipio",
    "município"
];
const CAMPOS_SGP_UF = [
    "endereco_uf",
    "enderecoUf",
    "uf",
    "estado",
    "cliente_uf",
    "clienteUf"
];
const CAMPOS_SGP_CEP = [
    "endereco_cep",
    "enderecoCep",
    "cep",
    "cliente_cep",
    "clienteCep"
];
const CAMPOS_SGP_LATITUDE = [
    "latitude",
    "lat",
    "Latitude",
    "LAT",
    "coordenada_latitude",
    "geo_latitude",
    "gps_latitude"
];
const CAMPOS_SGP_LONGITUDE = [
    "longitude",
    "lng",
    "lon",
    "Longitude",
    "LON",
    "coordenada_longitude",
    "geo_longitude",
    "gps_longitude"
];
const CAMPOS_SGP_COORDENADA_COMPOSTA = [
    "endereco_ll",
    "enderecoLl",
    "enderecoLatLong",
    "localizacao",
    "localização",
    "latitude_longitude",
    "lat_long",
    "coordenadas",
    "coordenada",
    "geolocalizacao",
    "geolocalização"
];
const CORES_ROTA_TECNICOS = [
    "#38bdf8",
    "#22c55e",
    "#a78bfa",
    "#f97316",
    "#14b8a6",
    "#eab308",
    "#ec4899",
    "#84cc16",
    "#06b6d4",
    "#f43f5e"
];
const CORES_ROTA_TECNICOS_FIXAS = {
    Alan: "#eab308",
    "Jeferson Dias": "#c2410c",
    Kesio: "#10b981",
    Leonardo: "#000000",
    Lucio: "#7c3aed",
    Noc: "#20bff3",
    Paulino: "#3b82f6",
    Washington: "#0f766e"
};
const COORDENADAS_REGIOES_ROTA = {
    FCO: { latitude: -23.3217, longitude: -46.7269, label: "Franco da Rocha" },
    FMO: { latitude: -23.2818, longitude: -46.7457, label: "Francisco Morato" },
    CAI: { latitude: -23.3643, longitude: -46.7408, label: "Caieiras" },
    LRJ: { latitude: -23.3472, longitude: -46.7561, label: "Laranjeiras" },
    IRACEMA: { latitude: -23.3328, longitude: -46.7163, label: "Iracema" },
    GERAL: { latitude: -23.3217, longitude: -46.7269, label: "Geral" }
};

async function fetchComTimeout(url, options, timeoutMs = 10000) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);

    try {
        return await fetch(url, {
            ...options,
            signal: controller.signal
        });
    } finally {
        clearTimeout(timeout);
    }
}

function getContratoId(os) {
    return os?.contrato || os?.id_contrato || os?.contrato_id || os?.idContrato || os?.idCliente || "";
}

function montarEnderecoContrato(contrato) {
    if (!contrato) return "";

    const enderecoPronto = pegarValorContrato(contrato, [
        "endereco",
        "endereco_completo",
        "enderecoCompleto",
        "cliente_endereco",
        "clienteEndereco"
    ]);

    if (enderecoPronto) return enderecoPronto;

    return [
        contrato.endereco_logradouro || contrato.enderecoLogradouro,
        contrato.endereco_numero || contrato.enderecoNumero,
        contrato.endereco_bairro || contrato.enderecoBairro || contrato.bairro,
        contrato.endereco_cidade || contrato.enderecoCidade,
        contrato.endereco_uf || contrato.enderecoUf
    ].filter(Boolean).join(", ");
}

function pegarValorContrato(contrato, campos) {
    if (!contrato) return "";

    for (const campo of campos) {
        if (contrato[campo]) return contrato[campo];
    }

    return buscarValorCampoSgp(contrato, campos)?.valor || "";
}

function normalizarChaveSgp(chave) {
    return String(chave || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
}

function isValorSgpPreenchido(valor) {
    if (valor === null || valor === undefined) return false;
    if (typeof valor === "string") return valor.trim() !== "";
    if (typeof valor === "number") return Number.isFinite(valor);
    if (typeof valor === "boolean") return true;
    return false;
}

function buscarValorCampoSgp(origem, campos, opcoes = {}) {
    if (!origem || typeof origem !== "object") return null;

    const camposLista = Array.isArray(campos) ? campos : [campos];
    const maxDepth = Number.isInteger(opcoes.maxDepth) ? opcoes.maxDepth : 7;
    const visitados = new Set();

    function visitar(alvoNormalizado, valor, caminho = "", depth = 0) {
        if (!valor || typeof valor !== "object" || depth > maxDepth || visitados.has(valor)) return null;
        visitados.add(valor);

        if (Array.isArray(valor)) {
            for (let index = 0; index < valor.length; index += 1) {
                const encontrado = visitar(alvoNormalizado, valor[index], `${caminho}[${index}]`, depth + 1);
                if (encontrado) return encontrado;
            }
            return null;
        }

        for (const [chave, item] of Object.entries(valor)) {
            const caminhoCampo = caminho ? `${caminho}.${chave}` : chave;
            if (normalizarChaveSgp(chave) === alvoNormalizado && isValorSgpPreenchido(item)) {
                return {
                    campo: chave,
                    caminho: caminhoCampo,
                    valor: item
                };
            }
        }

        for (const [chave, item] of Object.entries(valor)) {
            if (!item || typeof item !== "object") continue;
            const caminhoCampo = caminho ? `${caminho}.${chave}` : chave;
            const encontrado = visitar(alvoNormalizado, item, caminhoCampo, depth + 1);
            if (encontrado) return encontrado;
        }

        return null;
    }

    for (const campo of camposLista) {
        visitados.clear();
        const encontrado = visitar(normalizarChaveSgp(campo), origem);
        if (encontrado) return encontrado;
    }

    return null;
}

function obterValorCampoSgp(origem, campos) {
    const encontrado = buscarValorCampoSgp(origem, campos);
    return encontrado?.valor ?? "";
}

function limparValorEnderecoSgp(valor) {
    return String(valor || "")
        .replace(/\s+/g, " ")
        .trim();
}

function normalizarCepSgp(valor) {
    const digitos = String(valor || "").replace(/\D/g, "");
    if (digitos.length !== 8) return limparValorEnderecoSgp(valor);
    return `${digitos.slice(0, 5)}-${digitos.slice(5)}`;
}

function montarEnderecoCompletoNormalizadoSgp(partes, enderecoCompletoCampo = "") {
    const enderecoCampo = limparValorEnderecoSgp(enderecoCompletoCampo);
    const componentes = [
        partes.logradouro,
        partes.numero,
        partes.bairro,
        partes.cidade,
        partes.uf,
        partes.cep
    ].map(limparValorEnderecoSgp).filter(Boolean);

    if (componentes.length >= 2) return [...new Set(componentes)].join(", ");
    return enderecoCampo || componentes.join(", ");
}

function obterCoordenadaNormalizadaSgp(origem) {
    const latitudeInfo = buscarValorCampoSgp(origem, CAMPOS_SGP_LATITUDE);
    const longitudeInfo = buscarValorCampoSgp(origem, CAMPOS_SGP_LONGITUDE);

    if (latitudeInfo && longitudeInfo) {
        const coordenada = validarCoordenadaRota({
            latitude: String(latitudeInfo.valor).replace(",", "."),
            longitude: String(longitudeInfo.valor).replace(",", ".")
        });

        if (coordenada) {
            return {
                coordenada,
                campos: {
                    latitude: latitudeInfo,
                    longitude: longitudeInfo
                }
            };
        }
    }

    const compostoInfo = buscarValorCampoSgp(origem, CAMPOS_SGP_COORDENADA_COMPOSTA);
    const coordenadaComposta = compostoInfo ? parseCoordenadasRota(compostoInfo.valor) : null;

    if (coordenadaComposta) {
        return {
            coordenada: coordenadaComposta,
            campos: {
                coordenada: compostoInfo
            }
        };
    }

    return {
        coordenada: null,
        campos: {
            latitude: latitudeInfo,
            longitude: longitudeInfo,
            coordenada: compostoInfo
        }
    };
}

function normalizarEnderecoSgp(dadosSgp = {}) {
    const camposEndereco = {
        enderecoCompleto: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_ENDERECO_COMPLETO),
        logradouro: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_LOGRADOURO),
        numero: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_NUMERO),
        bairro: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_BAIRRO),
        cidade: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_CIDADE),
        uf: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_UF),
        cep: buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_CEP)
    };
    const partes = {
        logradouro: limparValorEnderecoSgp(camposEndereco.logradouro?.valor),
        numero: limparValorEnderecoSgp(camposEndereco.numero?.valor),
        bairro: limparValorEnderecoSgp(camposEndereco.bairro?.valor),
        cidade: limparValorEnderecoSgp(camposEndereco.cidade?.valor),
        uf: limparValorEnderecoSgp(camposEndereco.uf?.valor).toUpperCase(),
        cep: normalizarCepSgp(camposEndereco.cep?.valor)
    };
    const enderecoCompleto = montarEnderecoCompletoNormalizadoSgp(partes, camposEndereco.enderecoCompleto?.valor);
    const coordenadaInfo = obterCoordenadaNormalizadaSgp(dadosSgp);
    const coordenada = coordenadaInfo.coordenada;
    const localizacaoCampo = buscarValorCampoSgp(dadosSgp, CAMPOS_SGP_COORDENADA_COMPOSTA);

    return {
        ...partes,
        enderecoCompleto,
        endereco: enderecoCompleto,
        localizacao: coordenada ? `${coordenada.latitude},${coordenada.longitude}` : (localizacaoCampo?.valor || ""),
        latitude: coordenada?.latitude ?? null,
        longitude: coordenada?.longitude ?? null,
        fonteLocalizacao: coordenada ? "sgp" : "desconhecida",
        statusLocalizacao: coordenada ? "confirmada" : "sem_localizacao",
        camposEnderecoDetectados: Object.fromEntries(
            Object.entries(camposEndereco)
                .filter(([, info]) => info)
                .map(([chave, info]) => [chave, {
                    campo: info.campo,
                    caminho: info.caminho,
                    valor: info.valor
                }])
        ),
        camposCoordenadaDetectados: Object.fromEntries(
            Object.entries(coordenadaInfo.campos || {})
                .filter(([, info]) => info)
                .map(([chave, info]) => [chave, {
                    campo: info.campo,
                    caminho: info.caminho,
                    valor: info.valor
                }])
        )
    };
}

function montarComponentesEndereco(origem) {
    const normalizado = normalizarEnderecoSgp(origem);
    const logradouro = normalizado.logradouro;
    const numero = normalizado.numero;
    const bairro = normalizado.bairro;
    const cidade = normalizado.cidade;
    const uf = normalizado.uf;
    const cep = normalizado.cep;

    return { logradouro, numero, bairro, cidade, uf, cep };
}

function extrairTelefonesSgp(origem) {
    const lista = Array.isArray(origem?.telefones) ? origem.telefones : [];

    return [...new Set(
        lista
            .map(item => String(item?.contato || item || "").trim())
            .filter(Boolean)
    )];
}

function montarDadosContrato(origem) {
    const enderecoNormalizado = normalizarEnderecoSgp(origem);

    return {
        logradouro: enderecoNormalizado.logradouro,
        numero: enderecoNormalizado.numero,
        bairro: enderecoNormalizado.bairro,
        cidade: enderecoNormalizado.cidade,
        uf: enderecoNormalizado.uf,
        cep: enderecoNormalizado.cep,
        enderecoCompleto: enderecoNormalizado.enderecoCompleto || montarEnderecoContrato(origem),
        endereco: enderecoNormalizado.endereco || montarEnderecoContrato(origem),
        localizacao: enderecoNormalizado.localizacao,
        latitude: enderecoNormalizado.latitude,
        longitude: enderecoNormalizado.longitude,
        fonteLocalizacao: enderecoNormalizado.fonteLocalizacao,
        statusLocalizacao: enderecoNormalizado.statusLocalizacao,
        telefones: extrairTelefonesSgp(origem)
    };
}

function temDadosContrato(dados) {
    return Boolean(dados?.bairro || dados?.endereco || dados?.localizacao || validarCoordenadaRota(dados));
}

function salvarContratoCache(contratoId, dados, ttlMs = CONTRATO_CACHE_TTL_MS) {
    if (!contratoId) return;

    contratoCache.set(String(contratoId), {
        data: dados || {},
        expiresAt: Date.now() + ttlMs
    });
}

function obterContratoCache(contratoId) {
    if (!contratoId) return null;

    const cacheKey = String(contratoId);
    const cached = contratoCache.get(cacheKey);

    if (!cached) return null;

    if (cached.expiresAt <= Date.now()) {
        contratoCache.delete(cacheKey);
        return null;
    }

    return cached.data;
}

async function buscarDadosContrato(contratoId, opcoes = {}) {
    if (!contratoId) return {};

    const cacheKey = String(contratoId);
    const force = Boolean(opcoes.force);

    if (force) {
        contratoCache.delete(cacheKey);
        contratoRequests.delete(cacheKey);
    }

    const cached = obterContratoCache(cacheKey);

    if (!force && cached) {
        return cached;
    }

    if (!force && contratoRequests.has(cacheKey)) {
        return contratoRequests.get(cacheKey);
    }

    const request = (async () => {
        const formData = new FormData();
        formData.append("contrato", cacheKey);

        const res = await fetchComTimeout(`${SGP_BASE_URL}/api/ura/consultacliente/`, {
            method: "POST",
            headers: { "Authorization": `Basic ${basicAuth}` },
            body: formData
        }, CONTRATO_TIMEOUT_MS);

        if (!res.ok) {
            throw new Error(`Erro ${res.status} ao consultar contrato ${cacheKey}`);
        }

        const data = await res.json();
        const contratos = Array.isArray(data?.contratos) ? data.contratos : [];
        const contrato = contratos.find(item => String(
            item.id_contrato || item.contratoId || item.idContrato || item.contrato_id
        ) === cacheKey) || contratos[0];

        const dados = montarDadosContrato(contrato);
        salvarContratoCache(cacheKey, dados);

        return dados;
    })();

    contratoRequests.set(cacheKey, request);

    try {
        return await request;
    } finally {
        contratoRequests.delete(cacheKey);
    }
}

async function buscarDadosContratoBruto(contratoId) {
    if (!contratoId) return { resposta: null, contrato: null, contratos: [] };

    const cacheKey = String(contratoId);
    const formData = new FormData();
    formData.append("contrato", cacheKey);

    const res = await fetchComTimeout(`${SGP_BASE_URL}/api/ura/consultacliente/`, {
        method: "POST",
        headers: { "Authorization": `Basic ${basicAuth}` },
        body: formData
    }, CONTRATO_TIMEOUT_MS);

    if (!res.ok) {
        throw new Error(`Erro ${res.status} ao consultar contrato ${cacheKey}`);
    }

    const data = await res.json();
    const contratos = Array.isArray(data?.contratos) ? data.contratos : [];
    const contrato = contratos.find(item => String(
        item.id_contrato || item.contratoId || item.idContrato || item.contrato_id
    ) === cacheKey) || contratos[0] || null;

    return {
        resposta: data,
        contrato,
        contratos
    };
}

async function buscarDadosContratos(contratos, opcoes = {}) {
    const contratosUnicos = [...new Set(contratos.filter(Boolean).map(String))];
    const pares = [];

    for (let i = 0; i < contratosUnicos.length; i += CONTRATO_CONCORRENCIA) {
        const lote = contratosUnicos.slice(i, i + CONTRATO_CONCORRENCIA);

        const resultados = await Promise.all(lote.map(async (contratoId) => {
            try {
                return [contratoId, await buscarDadosContrato(contratoId, opcoes)];
            } catch (err) {
                console.error(`⚠️ Erro ao buscar dados do contrato ${contratoId}:`, err.message);
                salvarContratoCache(contratoId, {}, CONTRATO_ERRO_CACHE_TTL_MS);
                return [contratoId, {}];
            }
        }));

        pares.push(...resultados);
    }

    return new Map(pares);
}

function criarFormDataOrdemServico(status, date) {
    const formData = new FormData();
    formData.append("status", status);

    if (status === "1") {
        formData.append("data_finalizacao_inicio", date);
        formData.append("data_finalizacao_fim", date);
    }

    formData.append("data_agendamento_inicio", date);
    formData.append("data_agendamento_fim", date);

    return formData;
}

function getOrdemServicoStatusCacheKey(status, date) {
    return `${date}:${status}`;
}

async function buscarOrdensServicoStatus(status, date, opcoes = {}) {
    const cacheKey = getOrdemServicoStatusCacheKey(status, date);
    const force = Boolean(opcoes.force);

    if (force) {
        ordemServicoStatusCache.delete(cacheKey);
        ordemServicoStatusRequests.delete(cacheKey);
    }

    const cached = ordemServicoStatusCache.get(cacheKey);

    if (!force && cached && cached.expiresAt > Date.now()) {
        return cached.data;
    }

    if (!force && ordemServicoStatusRequests.has(cacheKey)) {
        return ordemServicoStatusRequests.get(cacheKey);
    }

    const request = (async () => {
        let res;
        let tentativa = 0;

        while (true) {
            res = await fetchComTimeout(`${SGP_BASE_URL}/api/ura/ordemservico/list/`, {
                method: "POST",
                headers: { "Authorization": `Basic ${basicAuth}` },
                body: criarFormDataOrdemServico(status, date)
            }, ORDEM_SERVICO_TIMEOUT_MS);

            if (res.status !== 429 || tentativa >= ORDEM_SERVICO_429_MAX_TENTATIVAS) break;

            tentativa += 1;
            const espera = ORDEM_SERVICO_429_BACKOFF_MS * tentativa;
            console.warn(`⚠️ SGP retornou 429 para OS status ${status} (tentativa ${tentativa}); nova tentativa em ${espera}ms.`);
            await new Promise(resolve => setTimeout(resolve, espera));
        }

        if (!res.ok) {
            throw new Error(`Erro ${res.status} ao buscar OS status ${status}`);
        }

        const data = await res.json();
        const ordens = data.ordens_servicos || [];

        if (Array.isArray(ordens) && ordens.length) {
            const amostra = ordens.slice(0, 3).map(os => ({
                id: os.id || os.id_os || os.id_ordem_servico,
                responsavel: os.responsavel,
                status: os.status,
                data_agendamento: os.data_agendamento
            }));
            debugAgenda("[AGENDA DEBUG] Amostra resposta SGP:", JSON.stringify(amostra));
        }

        ordemServicoStatusCache.set(cacheKey, {
            data: ordens,
            expiresAt: Date.now() + ORDEM_SERVICO_STATUS_CACHE_TTL_MS
        });

        return ordens;
    })();

    ordemServicoStatusRequests.set(cacheKey, request);

    try {
        return await request;
    } finally {
        ordemServicoStatusRequests.delete(cacheKey);
    }
}

async function buscarOrdensServico(dataFiltro, opcoes = {}) {
    const date = dataFiltro;
    const force = Boolean(opcoes.force);

    if (force) {
        agendaCache.delete(date);
        agendaRequests.delete(date);
    }

    const cached = agendaCache.get(date);

    if (!force && cached && cached.expiresAt > Date.now()) {
        return cached.data;
    }

    if (!force && agendaRequests.has(date)) {
        return agendaRequests.get(date);
    }

    const request = (async () => {
        console.log("🔄 Buscando API para a data:", date);

        const listas = [];

        for (let i = 0; i < STATUS_ORDEM_SERVICO.length; i++) {
            const status = STATUS_ORDEM_SERVICO[i];

            if (i > 0) {
                await new Promise(resolve => setTimeout(resolve, ORDEM_SERVICO_INTERVALO_ENTRE_STATUS_MS));
            }

            try {
                listas.push(await buscarOrdensServicoStatus(status, date, { force }));
            } catch (err) {
                console.error(`⚠️ Erro ao buscar OS status ${status}:`, err.message);

                const cacheAnterior = ordemServicoStatusCache.get(getOrdemServicoStatusCacheKey(status, date));
                if (cacheAnterior?.data?.length) {
                    console.warn(`⚠️ Usando cache anterior (stale) do status ${status} para ${date} após falha na consulta ao SGP.`);
                    listas.push(cacheAnterior.data);
                } else {
                    listas.push([]);
                }
            }
        }

        const ordens = listas.flat();
        const ordensAgenda = filtrarOrdensAgenda(ordens, opcoes);

        if (!force && !ordensAgenda.length) {
            const ultimaValida = obterUltimaAgendaValida(date, opcoes);

            if (ultimaValida?.data?.length) {
                console.warn(`⚠️ API retornou sem técnicos válidos para ${date}; usando última agenda válida com ${ultimaValida.totalAgenda} OS.`);
                return ultimaValida.data;
            }
        }

        if (!force && !ordensAgenda.length && cached?.data?.length && filtrarOrdensAgenda(cached.data, opcoes).length) {
            console.warn(`⚠️ API retornou sem técnicos válidos para ${date}; usando cache anterior com ${filtrarOrdensAgenda(cached.data, opcoes).length} OS.`);
            return cached.data;
        }

        if (ordensAgenda.length) {
            registrarUltimaAgendaValida(date, ordens, opcoes);
            agendaCache.set(date, {
                data: ordens,
                expiresAt: Date.now() + AGENDA_CACHE_TTL_MS
            });
        } else if (ordens.length) {
            console.warn(`⚠️ API retornou ${ordens.length} OS para ${date}, mas nenhuma com técnico mapeado; cache da agenda não foi substituído.`);
        }

        return ordens;
    })();

    agendaRequests.set(date, request);

    try {
        return await request;
    } finally {
        agendaRequests.delete(date);
    }
}

function obterMapaTecnicosAgenda(opcoes = {}) {
    return opcoes.incluirInstalacaoSemGps
        ? { ...MAPA_TECNICOS, ...MAPA_TECNICOS_INSTALACAO_SEM_GPS }
        : MAPA_TECNICOS;
}

function obterMapaTecnicosAgendaNormalizado(opcoes = {}) {
    return opcoes.incluirInstalacaoSemGps
        ? new Map([...MAPA_TECNICOS_NORMALIZADO, ...MAPA_TECNICOS_INSTALACAO_SEM_GPS_NORMALIZADO])
        : MAPA_TECNICOS_NORMALIZADO;
}

function obterTecnicoMapeadoAgenda(responsavel, opcoes = {}) {
    const identificador = normalizarIdentificadorSgpAgenda(responsavel);
    if (!identificador) return undefined;

    return obterMapaTecnicosAgendaNormalizado(opcoes).get(identificador);
}

function obterResponsavelSgpPorTecnicoAgenda(tecnicoId, opcoes = {}) {
    const valor = String(tecnicoId || "").trim();
    if (!valor) return "";

    const cadastro = obterCadastroTecnico(valor, { ...opcoes, incluirInstalacaoSemGps: true });
    if (cadastro?.usuarioSgp) return cadastro.usuarioSgp;

    const tecnicoMapeado = obterTecnicoMapeadoAgenda(valor, { ...opcoes, incluirInstalacaoSemGps: true });
    if (tecnicoMapeado) return valor;

    return "";
}

function normalizarAgendamentoSgp({ data, hora }) {
    const dataNormalizada = normalizarDataAgenda(data);
    const horaMatch = String(hora || "").trim().match(/^(\d{1,2})(?::(\d{2}))?$/);

    if (!dataNormalizada || !horaMatch) return null;

    const horas = Number(horaMatch[1]);
    const minutos = horaMatch[2] === undefined ? 0 : Number(horaMatch[2]);
    if (!Number.isInteger(horas) || !Number.isInteger(minutos)) return null;
    if (horas < 0 || horas > 23 || minutos < 0 || minutos > 59) return null;

    const horaNormalizada = `${String(horas).padStart(2, "0")}:${String(minutos).padStart(2, "0")}`;

    return {
        data: dataNormalizada,
        hora: horaNormalizada,
        dataHora: `${dataNormalizada} ${horaNormalizada}`
    };
}

function limparCachesAgendaSgp(data) {
    const dataNormalizada = normalizarDataAgenda(data);
    if (!dataNormalizada) return;

    agendaCache.delete(dataNormalizada);
    agendaRequests.delete(dataNormalizada);
    agendaUltimaValidaCache.delete(dataNormalizada);
    STATUS_ORDEM_SERVICO.forEach(status => {
        const cacheKey = getOrdemServicoStatusCacheKey(status, dataNormalizada);
        ordemServicoStatusCache.delete(cacheKey);
        ordemServicoStatusRequests.delete(cacheKey);
    });
}

function extrairMensagemErroSgp(dados, textoResposta) {
    if (dados && typeof dados === "object") {
        return dados.erro
            || dados.error
            || dados.detail
            || dados.msg
            || dados.message
            || "";
    }

    const texto = String(textoResposta || "").trim();
    if (!texto) return "";
    return texto.length > 240 ? `${texto.slice(0, 240)}...` : texto;
}

async function atualizarAgendamentoChamadoSgp({ osId, agendamento, tecnicoResponsavelSgpId }) {
    const formData = new FormData();
    formData.append("token", SGP_CHAMADO_UPDATE_TOKEN || OCORRENCIA_SGP_TOKEN);
    formData.append("app", SGP_CHAMADO_UPDATE_APP || OCORRENCIA_SGP_APP);
    formData.append("os_data_agendamento", agendamento.dataHora);
    formData.append("os_tecnico_responsavel", tecnicoResponsavelSgpId);

    const resposta = await fetchComTimeout(`${SGP_BASE_URL}/api/central/chamado/update/${encodeURIComponent(osId)}/`, {
        method: "POST",
        headers: formData.getHeaders(),
        body: formData
    }, SGP_CHAMADO_UPDATE_TIMEOUT_MS);

    const textoResposta = await resposta.text();
    let dados = null;

    if (textoResposta) {
        try {
            dados = JSON.parse(textoResposta);
        } catch {
            dados = null;
        }
    }

    if (!resposta.ok) {
        const mensagem = extrairMensagemErroSgp(dados, textoResposta);
        const credenciaisInvalidas = /credenciais|autentica/i.test(mensagem);
        const erro = new Error(
            credenciaisInvalidas
                ? "Token/app do SGP sem permissão para reagendar OS neste endpoint."
                : (mensagem || `SGP retornou erro ${resposta.status} ao reagendar a OS.`)
        );
        erro.statusCode = resposta.status;
        throw erro;
    }

    const mensagemErro = extrairMensagemErroSgp(dados, "");
    if (dados && (dados.erro || dados.error || dados.detail) && !String(mensagemErro).toLowerCase().includes("sucesso")) {
        const erro = new Error(mensagemErro || "SGP não aceitou o reagendamento da OS.");
        erro.statusCode = 400;
        throw erro;
    }

    return dados || { msg: textoResposta || "OS alterada com sucesso" };
}

function listarTecnicosInternosSemUsuarioSgp() {
    return TECNICOS_INTERNOS_SEM_USUARIO_SGP
        .map(item => ({
            id: String(item.id || item.nome || "").trim(),
            nome: String(item.nome || item.id || "").trim(),
            usuarioSgp: item.usuarioSgp || null,
            sgpAtivo: Boolean(item.sgpAtivo)
        }))
        .filter(item => item.id && item.nome);
}

function obterCadastroTecnicos(opcoes = {}) {
    const mapaTecnicos = obterMapaTecnicosAgenda(opcoes);
    const usuariosPorTecnico = new Map();

    Object.entries(mapaTecnicos).forEach(([usuarioSgp, tecnico]) => {
        const nome = String(tecnico || "").trim();
        const usuario = String(usuarioSgp || "").trim();
        if (nome && usuario && !usuariosPorTecnico.has(nome)) {
            usuariosPorTecnico.set(nome, usuario);
        }
    });

    const cadastros = [...new Set(Object.values(mapaTecnicos).map(nome => String(nome || "").trim()).filter(Boolean))]
        .map(nome => ({
            id: nome,
            nome,
            usuarioSgp: usuariosPorTecnico.get(nome) || null,
            sgpAtivo: Boolean(usuariosPorTecnico.get(nome)),
            sgpVinculado: Boolean(usuariosPorTecnico.get(nome)),
            statusIntegracaoSgp: "vinculado",
            statusIntegracaoSgpLabel: "Usuário SGP vinculado"
        }));
    const existentes = new Set(cadastros.map(item => normalizarChaveTecnicoAgendaTecnica(item.id)));

    listarTecnicosInternosSemUsuarioSgp().forEach(tecnico => {
        const chave = normalizarChaveTecnicoAgendaTecnica(tecnico.id);
        if (existentes.has(chave)) return;

        cadastros.push({
            ...tecnico,
            sgpVinculado: false,
            statusIntegracaoSgp: "usuario_nao_vinculado",
            statusIntegracaoSgpLabel: STATUS_USUARIO_SGP_NAO_VINCULADO
        });
        existentes.add(chave);
    });

    return cadastros.sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR"));
}

function obterCadastroTecnico(tecnicoId, opcoes = {}) {
    const chave = normalizarChaveTecnicoAgendaTecnica(tecnicoId);
    if (!chave) return null;

    return obterCadastroTecnicos(opcoes)
        .find(tecnico => normalizarChaveTecnicoAgendaTecnica(tecnico.id) === chave || normalizarChaveTecnicoAgendaTecnica(tecnico.nome) === chave)
        || null;
}

function montarInfoSgpTecnico(tecnicoId, opcoes = {}) {
    const cadastro = obterCadastroTecnico(tecnicoId, opcoes);
    const usuarioSgp = cadastro?.usuarioSgp || null;
    const sgpAtivo = Boolean(cadastro?.sgpAtivo && usuarioSgp);
    const sgpVinculado = Boolean(usuarioSgp && sgpAtivo);

    return {
        usuarioSgp,
        sgpAtivo,
        sgpVinculado,
        statusIntegracaoSgp: sgpVinculado ? "vinculado" : "usuario_nao_vinculado",
        statusIntegracaoSgpLabel: sgpVinculado ? "Usuário SGP vinculado" : STATUS_USUARIO_SGP_NAO_VINCULADO
    };
}

function obterTecnicosValidos(opcoes = {}) {
    return new Set(obterCadastroTecnicos(opcoes).map(tecnico => tecnico.id));
}

function obterTecnicosValidosHorariosSemana() {
    return obterTecnicosValidos({ incluirInstalacaoSemGps: true });
}

function normalizarChaveTecnicoAgendaTecnica(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");
}

function isTecnicoOcultoAgendaTecnica(valor) {
    const chave = normalizarChaveTecnicoAgendaTecnica(valor);
    return Boolean(chave && TECNICOS_OCULTOS_AGENDA_TECNICA.has(chave));
}

function isTecnicoVisivelAgendaTecnica(tecnico) {
    return !isTecnicoOcultoAgendaTecnica(tecnico);
}

function compararTecnicosAgendaTecnica(a, b) {
    const prioridadeA = normalizarChaveTecnicoAgendaTecnica(a) === "noc" ? 0 : 1;
    const prioridadeB = normalizarChaveTecnicoAgendaTecnica(b) === "noc" ? 0 : 1;
    if (prioridadeA !== prioridadeB) return prioridadeA - prioridadeB;

    return String(a || "").localeCompare(String(b || ""), "pt-BR");
}

function obterTecnicosValidosAgendaTecnica() {
    return new Set([...obterTecnicosValidosHorariosSemana()].filter(isTecnicoVisivelAgendaTecnica));
}

function obterTecnicosValidosCoordenadorRota(filtros = {}) {
    if (isEscopoInstalacaoCoordenadorRota(filtros)) {
        return new Set(TECNICOS_COORDENADOR_ROTA_INSTALACAO);
    }

    return obterTecnicosValidos({ incluirInstalacaoSemGps: true });
}

function isTecnicoSemRastreamentoGps(tecnicoId) {
    return TECNICOS_SEM_RASTREAMENTO_GPS.has(String(tecnicoId || "").trim());
}

function filtrarOrdensAgenda(ordens, opcoes = {}) {
    const resultado = ordens.filter(os => {
        const mapeado = obterTecnicoMapeadoAgenda(os.responsavel, opcoes);
        if (!mapeado && os.responsavel) {
            debugAgenda("[AGENDA DEBUG] OS rejeitada no filtro de técnicos:", {
                osId: os.id || os.id_os || os.id_ordem_servico,
                responsavelSgp: os.responsavel,
                tecnicoMapeado: null
            });
        }
        return mapeado;
    });

    const rejeitadas = ordens.length - resultado.length;
    if (rejeitadas > 0) {
        debugAgenda(`[AGENDA DEBUG] ${rejeitadas} OS rejeitadas por técnico não mapeado.`);
    }

    return resultado;
}

function filtrarOrdensAgendaTecnica(ordens, opcoes = {}) {
    return filtrarOrdensAgenda(ordens, opcoes)
        .filter(os => !isTecnicoOcultoAgendaTecnica(obterTecnicoMapeadoAgenda(os.responsavel, opcoes) || os.responsavel));
}

function filtrarAgendaMontadaAgendaTecnica(agenda = []) {
    return (Array.isArray(agenda) ? agenda : [])
        .filter(os => !isTecnicoOcultoAgendaTecnica(os?.tecnico || os?.tecnicoId || os?.technicianId));
}

function obterTecnicoHorarioAgendaTecnica(item = {}) {
    return item?.technicianId
        || item?.technician_id
        || item?.id
        || item?.name
        || item?.nome
        || "";
}

function filtrarHorarioTecnicosAgendaTecnica(schedule = {}) {
    if (!schedule || typeof schedule !== "object") return schedule;

    const filtrarTecnico = tecnico => !isTecnicoOcultoAgendaTecnica(obterTecnicoHorarioAgendaTecnica(tecnico));
    const shifts = Array.isArray(schedule.shifts)
        ? schedule.shifts.map(shift => {
            const technicians = (Array.isArray(shift.technicians) ? shift.technicians : [])
                .filter(filtrarTecnico);

            return {
                ...shift,
                technicians,
                tecnicos: technicians
                    .map(tecnico => tecnico.name || tecnico.technicianId || tecnico.technician_id || tecnico.id)
                    .filter(Boolean)
            };
        })
        : schedule.shifts;
    const availableTechnicians = Array.isArray(schedule.availableTechnicians)
        ? schedule.availableTechnicians.filter(filtrarTecnico)
        : schedule.availableTechnicians;

    return {
        ...schedule,
        shifts,
        availableTechnicians
    };
}

function registrarUltimaAgendaValida(data, ordens, opcoes = {}) {
    const ordensAgenda = filtrarOrdensAgenda(ordens, opcoes);

    if (!ordensAgenda.length) return null;

    const registro = {
        data: ordens,
        totalAgenda: ordensAgenda.length,
        updatedAt: Date.now()
    };

    agendaUltimaValidaCache.set(data, registro);
    return registro;
}

function obterUltimaAgendaValida(data, opcoes = {}) {
    const registro = agendaUltimaValidaCache.get(data);

    if (registro?.data?.length && filtrarOrdensAgenda(registro.data, opcoes).length) {
        return registro;
    }

    return null;
}

function obterContratosPendentes(ordens) {
    const pendentes = [];

    for (const os of ordens) {
        const contratoId = getContratoId(os);
        if (!contratoId) continue;

        const dadosOS = montarDadosContrato(os);

        if (temDadosContrato(dadosOS)) {
            salvarContratoCache(contratoId, dadosOS);
            continue;
        }

        if (!obterContratoCache(contratoId)) {
            pendentes.push(contratoId);
        }
    }

    return [...new Set(pendentes.map(String))];
}

async function montarAgenda(ordens, dadosContratos = new Map(), opcoes = {}) {
    const dataReferencia = opcoes.dataFiltro || obterHojeBrasilBackend();
    let horariosEfetivos = new Map();
    const localizacoesStore = lerStoreLocalizacoesChamados();
    const localizacoesCache = listarLocalizacoesChamadosPorChave(localizacoesStore);
    let localizacoesAlteradas = false;

    try {
        horariosEfetivos = await obterHorariosEfetivosPorTecnicoData(dataReferencia, obterTecnicosValidos());
    } catch (err) {
        console.error("Não foi possível calcular horários efetivos dos técnicos:", err.message);
    }

    const agenda = ordens.map(os => {
        const contratoId = getContratoId(os);
        const dadosOS = montarDadosContrato(os);
        const dadosContrato =
            dadosContratos.get(String(contratoId)) ||
            obterContratoCache(contratoId) ||
            dadosOS ||
            {};
        const tecnico = obterTecnicoMapeadoAgenda(os.responsavel, opcoes);
        const horarioEfetivo = horariosEfetivos.get(tecnico) || null;

        debugAgenda("[AGENDA DEBUG] Montando agenda:", {
            osId: os.id || os.id_os || os.id_ordem_servico,
            responsavelSgp: os.responsavel,
            tecnicoMapeado: tecnico,
            horarioEfetivo: horarioEfetivo ? `${horarioEfetivo.startTime} às ${horarioEfetivo.endTime}` : null
        });
        const enderecoCompleto = dadosContrato.enderecoCompleto || dadosContrato.endereco || "";
        const chamadoId = obterChamadoIdCoordenadorRota({
            id: os.id || os.id_os || os.id_ordem_servico || os.ordem_servico || "",
            idOrdemServico: os.id || os.id_os || os.id_ordem_servico || os.ordem_servico || "",
            idCliente: contratoId
        }, dataReferencia);
        const localizacao = obterLocalizacaoChamadoPadronizada({
            chamadoId,
            enderecoCompleto,
            localizacaoApi: dadosContrato.localizacao || os.localizacao,
            localizacoesCache,
            localizacoesStore,
            onLocalizacaoCacheAlterada: () => {
                localizacoesAlteradas = true;
            }
        });

        return {
            id: os.id || os.id_os || os.id_ordem_servico || os.ordem_servico || contratoId,
            tecnico,
            tecnicoResponsavelSgpId: os.responsavel || "",
            tecnico_responsavel_sgp_id: os.responsavel || "",
            motivo: os.motivo,
            idCliente: contratoId,
            idOrdemServico: os.id || os.id_os || os.id_ordem_servico || os.ordem_servico || "",
            pop: os.pop,
            cliente: os.cliente,
            bairro: dadosContrato.bairro || "",
            logradouro: dadosContrato.logradouro || "",
            numero: dadosContrato.numero || "",
            cidade: dadosContrato.cidade || "",
            uf: dadosContrato.uf || "",
            cep: dadosContrato.cep || "",
            enderecoCompleto,
            endereco: dadosContrato.endereco || "",
            localizacao: dadosContrato.localizacao || os.localizacao || "",
            latitude: localizacao.latitude,
            longitude: localizacao.longitude,
            status_localizacao: localizacao.status_localizacao,
            statusLocalizacao: localizacao.status_localizacao,
            origemCoordenada: localizacao.origemCoordenada,
            fonteLocalizacao: localizacao.fonteLocalizacao,
            motivoSemLocalizacao: !validarCoordenadaRota(localizacao)
                ? (localizacao.observacaoLocalizacao || "SGP não retornou latitude/longitude")
                : "",
            possuiLocalizacao: Boolean(validarCoordenadaRota(localizacao)),
            status: os.status,
            hora: os.hora_agendamento,
            horarioEfetivo,
            horario_efetivo: horarioEfetivo,
            shiftId: horarioEfetivo?.shiftId || null,
            turnoNome: horarioEfetivo?.shiftName || null,
            turnoHorario: horarioEfetivo ? `${horarioEfetivo.startTime} às ${horarioEfetivo.endTime}` : null,
            data_finalizado: os.hora_finalizacao,
            Checkin: os.Checkin
        };
    });

    if (localizacoesAlteradas) {
        salvarStoreLocalizacoesChamados(localizacoesStore);
    }

    return agenda;
}

async function buscarAgenda(dataFiltro, opcoes = {}) {
    try {
        const inicio = Date.now();
        const force = Boolean(opcoes.force);
        let todasOS = await buscarOrdensServico(dataFiltro, opcoes);
        let ordensAgenda = filtrarOrdensAgenda(todasOS, opcoes);

        if (!force && !ordensAgenda.length) {
            const ultimaValida = obterUltimaAgendaValida(dataFiltro, opcoes);

            if (ultimaValida?.data?.length) {
                console.warn(`⚠️ Montagem da agenda sem técnicos válidos para ${dataFiltro}; reaproveitando última agenda válida.`);
                todasOS = ultimaValida.data;
                ordensAgenda = filtrarOrdensAgenda(todasOS, opcoes);
            }
        } else {
            registrarUltimaAgendaValida(dataFiltro, todasOS, opcoes);
        }

        const contratosPendentes = opcoes.contratosSempre
            ? [...new Set(ordensAgenda.map(getContratoId).filter(Boolean).map(String))]
            : obterContratosPendentes(ordensAgenda);
        const agenda = await montarAgenda(ordensAgenda, new Map(), { ...opcoes, dataFiltro });

        const bairrosEncontrados = agenda.filter(os => os.bairro).length;
        console.log(`✅ ${agenda.length} OS encontradas para ${dataFiltro} (${bairrosEncontrados} com bairro) em ${Date.now() - inicio}ms`);

        return {
            agenda,
            ordensAgenda,
            contratosPendentes
        };

    } catch (err) {
        console.error("🔥 Erro:", err);
        return {
            agenda: [],
            ordensAgenda: [],
            contratosPendentes: []
        };
    }
}

async function enriquecerAgendaComContratos(ordensAgenda, contratosPendentes, opcoes = {}) {
    if (!contratosPendentes.length) return null;

    const inicio = Date.now();
    const dadosContratos = await buscarDadosContratos(contratosPendentes, opcoes);
    const agenda = await montarAgenda(ordensAgenda, dadosContratos, opcoes);
    const bairrosEncontrados = agenda.filter(os => os.bairro).length;

    console.log(`🏁 Bairros atualizados (${bairrosEncontrados}/${agenda.length}) em ${Date.now() - inicio}ms`);
    return agenda;
}

function criarChaveAgendaEnriquecida(dataFiltro, ordensAgenda = [], contratosPendentes = []) {
    const resumo = {
        dataFiltro,
        ordens: (Array.isArray(ordensAgenda) ? ordensAgenda : []).map(os => [
            os.id,
            os.id_os,
            os.id_ordem_servico,
            os.ordem_servico,
            getContratoId(os),
            os.status,
            os.hora_agendamento,
            os.Checkin,
            os.hora_finalizacao,
            os.responsavel
        ])
    };

    return crypto
        .createHash("sha1")
        .update(JSON.stringify(resumo))
        .digest("hex");
}

async function enriquecerAgendaComContratosCompartilhado(ordensAgenda, contratosPendentes, opcoes = {}) {
    if (!contratosPendentes.length) return null;

    const dataFiltro = opcoes.dataFiltro || "";
    const cacheKey = criarChaveAgendaEnriquecida(dataFiltro, ordensAgenda, contratosPendentes);
    const cached = agendaEnriquecidaCache.get(cacheKey);

    if (cached && cached.expiresAt > Date.now()) {
        return cached.data;
    }

    if (agendaEnriquecidaRequests.has(cacheKey)) {
        return agendaEnriquecidaRequests.get(cacheKey);
    }

    const request = enriquecerAgendaComContratos(ordensAgenda, contratosPendentes, opcoes)
        .then((agenda) => {
            if (agenda) {
                agendaEnriquecidaCache.set(cacheKey, {
                    data: agenda,
                    expiresAt: Date.now() + AGENDA_ENRIQUECIDA_CACHE_TTL_MS
                });
            }

            return agenda;
        })
        .finally(() => {
            agendaEnriquecidaRequests.delete(cacheKey);
        });

    agendaEnriquecidaRequests.set(cacheKey, request);
    return request;
}

async function buscarAgendaSgpAtualizada(dataFiltro, opcoes = {}) {
    const resultado = await buscarAgenda(dataFiltro, {
        ...opcoes,
        force: true,
        contratosSempre: true
    });
    const agendaEnriquecida = await enriquecerAgendaComContratos(
        resultado.ordensAgenda,
        resultado.contratosPendentes,
        { ...opcoes, force: true, dataFiltro }
    );

    return {
        ...resultado,
        agenda: agendaEnriquecida || resultado.agenda
    };
}

function listarDatasPeriodoImpactoBairros(dataInicio, dataFim) {
    const inicio = criarDataUtcMassivo(dataInicio);
    const fim = criarDataUtcMassivo(dataFim);

    if (!inicio || !fim || inicio.getTime() > fim.getTime()) return [];

    const datas = [];
    const atual = new Date(inicio.getTime());

    while (atual.getTime() <= fim.getTime()) {
        datas.push(formatarDataUtcMassivo(atual));
        atual.setUTCDate(atual.getUTCDate() + 1);
    }

    return datas;
}

function normalizarPeriodoImpactoBairros(query = {}) {
    const startDate = normalizarDataAusencia(query.startDate || query.dataInicio);
    const endDate = normalizarDataAusencia(query.endDate || query.dataFim);
    const datas = listarDatasPeriodoImpactoBairros(startDate, endDate);

    if (!startDate || !endDate || !datas.length) {
        const erro = new Error("Informe startDate e endDate válidos no formato YYYY-MM-DD.");
        erro.statusCode = 400;
        throw erro;
    }

    if (datas.length > 7) {
        const erro = new Error("A análise de impacto por bairro aceita no máximo 7 dias por consulta.");
        erro.statusCode = 400;
        throw erro;
    }

    return {
        startDate,
        endDate,
        datas
    };
}

function deslocarDataImpactoBairros(dataISO, dias) {
    const data = criarDataUtcMassivo(dataISO);
    if (!data) return "";
    data.setUTCDate(data.getUTCDate() + dias);
    return formatarDataUtcMassivo(data);
}

async function montarChamadosImpactoBairrosPeriodo(datas, opcoes = {}) {
    const resultados = await Promise.all(datas.map(async data => {
        const resultado = await buscarAgenda(data, {
            force: opcoes.force,
            contratosSempre: true
        });
        const agendaEnriquecida = await enriquecerAgendaComContratos(
            resultado.ordensAgenda,
            resultado.contratosPendentes,
            { dataFiltro: data, force: opcoes.force }
        );
        const agenda = filtrarAgendaMontadaAgendaTecnica(agendaEnriquecida || resultado.agenda)
            .map(item => ({
                ...item,
                data,
                dataAgendamento: data,
                data_agendamento: data
            }));

        return {
            data,
            agenda,
            resumo: {
                data,
                total: agenda.length,
                bairrosComBairro: agenda.filter(item => item.bairro).length
            }
        };
    }));

    return {
        chamados: resultados.flatMap(item => item.agenda),
        dias: resultados.map(item => item.resumo)
    };
}

async function montarImpactoBairrosPeriodo(periodo, opcoes = {}) {
    const force = Boolean(opcoes.force);
    const cacheKey = `${periodo.startDate}|${periodo.endDate}|previous:${opcoes.includePrevious !== false}`;
    const cached = impactoBairrosSemanaCache.get(cacheKey);

    if (!force && cached && cached.expiresAt > Date.now()) {
        return {
            ...cached.data,
            cache: {
                hit: true,
                expiresAt: new Date(cached.expiresAt).toISOString()
            }
        };
    }

    const atualPromise = montarChamadosImpactoBairrosPeriodo(periodo.datas, { force });
    const previousStartDate = opcoes.includePrevious !== false
        ? deslocarDataImpactoBairros(periodo.startDate, -periodo.datas.length)
        : "";
    const previousEndDate = opcoes.includePrevious !== false
        ? deslocarDataImpactoBairros(periodo.endDate, -periodo.datas.length)
        : "";
    const previousDatas = opcoes.includePrevious !== false
        ? listarDatasPeriodoImpactoBairros(previousStartDate, previousEndDate)
        : [];
    const anteriorPromise = opcoes.includePrevious !== false
        ? montarChamadosImpactoBairrosPeriodo(previousDatas, { force })
        : Promise.resolve(null);
    const [atual, anterior] = await Promise.all([atualPromise, anteriorPromise]);
    const resposta = {
        startDate: periodo.startDate,
        endDate: periodo.endDate,
        dias: atual.dias,
        chamados: atual.chamados,
        generatedAt: new Date().toISOString()
    };

    if (anterior) {
        resposta.previous = {
            startDate: previousStartDate,
            endDate: previousEndDate,
            dias: anterior.dias,
            chamados: anterior.chamados
        };
    }

    const expiresAt = Date.now() + (WEEKLY_NEIGHBORHOOD_CACHE_MINUTES * 60 * 1000);
    impactoBairrosSemanaCache.set(cacheKey, {
        data: resposta,
        expiresAt
    });

    return {
        ...resposta,
        cache: {
            hit: false,
            expiresAt: new Date(expiresAt).toISOString()
        }
    };
}

function criarDataUtcMassivo(data) {
    const valor = normalizarDataAusencia(data);
    if (!valor) return null;

    const [ano, mes, dia] = valor.split("-").map(Number);
    return new Date(Date.UTC(ano, mes - 1, dia));
}

function formatarDataUtcMassivo(data) {
    return [
        data.getUTCFullYear(),
        String(data.getUTCMonth() + 1).padStart(2, "0"),
        String(data.getUTCDate()).padStart(2, "0")
    ].join("-");
}

function adicionarDiasMassivo(data, dias) {
    const copia = new Date(data.getTime());
    copia.setUTCDate(copia.getUTCDate() + dias);
    return copia;
}

function normalizarPeriodoMassivosHistorico(query = {}) {
    const dataFimInformada = normalizarDataAusencia(query.dataFim);
    const dataFim = criarDataUtcMassivo(dataFimInformada || obterDataHojeBrasil());
    const dataInicioInformada = normalizarDataAusencia(query.dataInicio);
    const dataInicio = dataInicioInformada
        ? criarDataUtcMassivo(dataInicioInformada)
        : adicionarDiasMassivo(dataFim, -(MASSIVOS_HISTORICO_DIAS_PADRAO - 1));

    if (!dataInicio || !dataFim) {
        const erro = new Error("Informe datas válidas no formato YYYY-MM-DD.");
        erro.statusCode = 400;
        throw erro;
    }

    if (dataInicio.getTime() > dataFim.getTime()) {
        const erro = new Error("A data inicial não pode ser maior que a data final.");
        erro.statusCode = 400;
        throw erro;
    }

    const dias = Math.floor((dataFim.getTime() - dataInicio.getTime()) / 86400000) + 1;
    if (dias > MASSIVOS_HISTORICO_DIAS_MAXIMO) {
        const erro = new Error(`Consulte no máximo ${MASSIVOS_HISTORICO_DIAS_MAXIMO} dias por vez.`);
        erro.statusCode = 400;
        throw erro;
    }

    return {
        dataInicio: formatarDataUtcMassivo(dataInicio),
        dataFim: formatarDataUtcMassivo(dataFim),
        dias
    };
}

function normalizarTipoMassivo(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/\s+/g, " ")
        .trim()
        .toUpperCase();
}

function isOcorrenciaMassivoSgp(ocorrencia) {
    const tipo = normalizarTipoMassivo(ocorrencia?.tipo);
    if (!tipo) return false;

    return TIPOS_MASSIVO_SGP.some(tipoMassivo => normalizarTipoMassivo(tipoMassivo) === tipo)
        || tipo.includes("MASSIVO");
}

function obterLocalMassivoSgp(ocorrencia) {
    return String(
        ocorrencia?.pop
        || ocorrencia?.regiao
        || ocorrencia?.região
        || ocorrencia?.cidade
        || ocorrencia?.local
        || "Local não informado"
    ).trim();
}

async function buscarOcorrenciasSgp(bodyBase = {}) {
    const ocorrencias = [];
    let offset = 0;
    let total = Infinity;
    let pagina = 0;

    while (offset < total && pagina < 30) {
        const body = {
            token: OCORRENCIA_SGP_TOKEN,
            app: OCORRENCIA_SGP_APP,
            limit: OCORRENCIA_SGP_LIMIT,
            offset,
            ...bodyBase
        };

        const resposta = await fetchComTimeout(OCORRENCIA_SGP_API_URL, {
            method: "POST",
            headers: {
                "Authorization": `Basic ${basicAuth}`,
                "Content-Type": "application/json"
            },
            body: JSON.stringify(body)
        }, OCORRENCIA_TIMEOUT_MS);

        if (!resposta.ok) {
            throw new Error(`Erro ${resposta.status} ao buscar ocorrências no SGP.`);
        }

        const dados = await resposta.json();
        const lista = Array.isArray(dados?.ocorrencias) ? dados.ocorrencias : [];
        const totalApi = Number(dados?.paginacao?.total);
        total = Number.isFinite(totalApi) ? totalApi : lista.length;
        ocorrencias.push(...lista);

        if (!lista.length || lista.length < OCORRENCIA_SGP_LIMIT) break;

        offset += OCORRENCIA_SGP_LIMIT;
        pagina += 1;
    }

    return ocorrencias;
}

// ===================== MAPA DE CHAMADOS (N1/N2) =====================

const MAPA_CHAMADOS_STATUS_ABERTOS = ["0", "2", "3"]; // Aberta, Em execução, Pendente
const MAPA_CHAMADOS_FILAS_SGP = {
    "SUPORTE N1": "N1",
    "SUPORTE N2": "N2",
    "SUPORTE TÉCNICO": "ST"
};
const MAPA_CHAMADOS_BASE_CACHE_TTL_MS = Number(process.env.MAPA_CHAMADOS_CACHE_TTL_MS || 120 * 1000);
const MAPA_CHAMADOS_REFRESH_BACKGROUND_MS = Number(process.env.MAPA_CHAMADOS_REFRESH_INTERVAL_MS || 45 * 1000);
const MAPA_CHAMADOS_CONCENTRACAO_RAIO_KM = Number(process.env.MAPA_CHAMADOS_CONCENTRACAO_RAIO_KM || 0.5);
const MAPA_CHAMADOS_CONCENTRACAO_MIN_CHAMADOS = Number(process.env.MAPA_CHAMADOS_CONCENTRACAO_MIN_CHAMADOS || 3);
const MAPA_CHAMADOS_GEOCODE_PROVIDER_URL = "https://nominatim.openstreetmap.org/search";
const MAPA_CHAMADOS_GEOCODE_USER_AGENT = "AgendaTecnicosAcessanet-MapaChamados/1.0";
const MAPA_CHAMADOS_GEOCODE_TIMEOUT_MS = 8000;
const MAPA_CHAMADOS_GEOCODE_MIN_INTERVALO_MS = 1100;
const MAPA_CHAMADOS_TELEFONE_PERMISSION = "agenda_tecnicos_ver_telefone_mapa_chamados";
const MAPA_CHAMADOS_TEMPO_ANTIGO_MINUTOS = 240;
const MAPA_CHAMADOS_ENRIQUECIMENTO_CONCORRENCIA = 12;

let mapaChamadosBaseCache = null;
let mapaChamadosBaseRequestEmAndamento = null;
let mapaChamadosUltimoGeocodeEm = 0;

function obterFilaChamadoMapa(tipoOcorrencia) {
    return MAPA_CHAMADOS_FILAS_SGP[String(tipoOcorrencia || "").trim().toUpperCase()] || null;
}

function parseDataHoraOcorrenciaMapa(ocorrencia) {
    const numero = String(ocorrencia?.numero || "").trim();

    if (/^\d{12}$/.test(numero)) {
        const ano = 2000 + Number(numero.slice(0, 2));
        const mes = Number(numero.slice(2, 4)) - 1;
        const dia = Number(numero.slice(4, 6));
        const hora = Number(numero.slice(6, 8));
        const minuto = Number(numero.slice(8, 10));
        const segundo = Number(numero.slice(10, 12));
        const data = new Date(ano, mes, dia, hora, minuto, segundo);

        if (!Number.isNaN(data.getTime())) return data;
    }

    const dataCadastro = String(ocorrencia?.data_cadastro || "").trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(dataCadastro)) {
        const data = new Date(`${dataCadastro}T00:00:00`);
        if (!Number.isNaN(data.getTime())) return data;
    }

    return null;
}

function formatarDataLocalMapa(data) {
    const ano = data.getFullYear();
    const mes = String(data.getMonth() + 1).padStart(2, "0");
    const dia = String(data.getDate()).padStart(2, "0");
    return `${ano}-${mes}-${dia}`;
}

function formatarTempoAbertoLabelMapa(minutosTotais) {
    if (!Number.isFinite(minutosTotais) || minutosTotais < 0) return "-";

    const minutos = Math.floor(minutosTotais);

    if (minutos < 60) return `${minutos} min`;

    const horas = Math.floor(minutos / 60);
    const minutosRestantes = minutos % 60;

    if (horas < 24) {
        return minutosRestantes > 0 ? `${horas}h ${minutosRestantes}min` : `${horas}h`;
    }

    const dias = Math.floor(horas / 24);
    return `${dias} ${dias === 1 ? "dia" : "dias"}`;
}

function extrairMotivoOcorrenciaMapa(ocorrencia) {
    const conteudo = String(ocorrencia?.conteudo || "");
    const linhas = conteudo.split(/\r?\n/).map(linha => linha.trim()).filter(Boolean);
    const diagnostico = linhas.find(linha => /^diagn[oó]stico\s*:/i.test(linha));

    if (diagnostico) {
        return diagnostico.replace(/^diagn[oó]stico\s*:/i, "").trim().slice(0, 160) || ocorrencia?.tipo || "-";
    }

    const semCabecalho = linhas.filter(linha => !/^(nome|contato|telefone|status do equipamento|protocolo|data\/hora)/i.test(linha));
    const base = semCabecalho[0] || linhas[0] || ocorrencia?.tipo || "-";

    return base.slice(0, 160);
}

let mapaChamadosGeocodeFila = Promise.resolve();

async function geocodificarEnderecoMapaChamadosSerializado(enderecoCompleto) {
    const agora = Date.now();
    const espera = Math.max(0, MAPA_CHAMADOS_GEOCODE_MIN_INTERVALO_MS - (agora - mapaChamadosUltimoGeocodeEm));
    if (espera > 0) await new Promise(resolve => setTimeout(resolve, espera));
    mapaChamadosUltimoGeocodeEm = Date.now();

    try {
        const params = new URLSearchParams({
            q: enderecoCompleto,
            format: "json",
            limit: "1",
            countrycodes: "br"
        });

        const resposta = await fetchComTimeout(`${MAPA_CHAMADOS_GEOCODE_PROVIDER_URL}?${params.toString()}`, {
            headers: { "User-Agent": MAPA_CHAMADOS_GEOCODE_USER_AGENT }
        }, MAPA_CHAMADOS_GEOCODE_TIMEOUT_MS);

        if (!resposta.ok) return null;

        const dados = await resposta.json();
        const item = Array.isArray(dados) ? dados[0] : null;
        if (!item) return null;

        return validarCoordenadaRota({ latitude: item.lat, longitude: item.lon });
    } catch (err) {
        console.warn("⚠️ Falha ao geocodificar endereço do mapa de chamados:", err.message);
        return null;
    }
}

function geocodificarEnderecoMapaChamados(enderecoCompleto) {
    if (!enderecoCompleto) return Promise.resolve(null);

    const resultado = mapaChamadosGeocodeFila.then(
        () => geocodificarEnderecoMapaChamadosSerializado(enderecoCompleto),
        () => geocodificarEnderecoMapaChamadosSerializado(enderecoCompleto)
    );
    mapaChamadosGeocodeFila = resultado.catch(() => {});

    return resultado;
}

async function resolverLocalizacaoChamadoMapa(contratoId, dadosContrato) {
    const coordenadaSgp = validarCoordenadaRota(dadosContrato);
    if (coordenadaSgp) {
        return { ...coordenadaSgp, fonteLocalizacao: "sgp", statusLocalizacao: "confirmada" };
    }

    const enderecoCompleto = dadosContrato?.enderecoCompleto || dadosContrato?.endereco || "";
    if (!enderecoCompleto || !contratoId) {
        return { latitude: null, longitude: null, fonteLocalizacao: "desconhecida", statusLocalizacao: "sem_localizacao" };
    }

    const addressHash = calcularAddressHash(enderecoCompleto);
    const cache = await obterGeocodeCache(contratoId).catch(() => null);

    if (cache && String(cache.address_hash) === addressHash) {
        return {
            latitude: Number(cache.latitude),
            longitude: Number(cache.longitude),
            fonteLocalizacao: "geocodificacao_cache",
            statusLocalizacao: "confirmada"
        };
    }

    const coordenadaGeocodificada = await geocodificarEnderecoMapaChamados(enderecoCompleto);

    if (!coordenadaGeocodificada) {
        return { latitude: null, longitude: null, fonteLocalizacao: "desconhecida", statusLocalizacao: "sem_localizacao" };
    }

    await salvarGeocodeCache({
        contratoId,
        addressHash,
        latitude: coordenadaGeocodificada.latitude,
        longitude: coordenadaGeocodificada.longitude,
        provider: "nominatim"
    }).catch(err => console.warn("⚠️ Falha ao salvar cache de geocodificação do mapa de chamados:", err.message));

    return { ...coordenadaGeocodificada, fonteLocalizacao: "geocodificacao", statusLocalizacao: "confirmada" };
}

async function buscarChamadosSuporteAbertosSgp() {
    const listas = await Promise.all(
        MAPA_CHAMADOS_STATUS_ABERTOS.map(status => buscarOcorrenciasSgp({ status }).catch(err => {
            console.error(`⚠️ Erro ao buscar ocorrências do mapa de chamados (status ${status}):`, err.message);
            return [];
        }))
    );

    const vistos = new Set();
    const resultado = [];

    for (const lista of listas) {
        for (const ocorrencia of lista) {
            const fila = obterFilaChamadoMapa(ocorrencia?.tipo);
            if (!fila) continue;

            const id = String(ocorrencia?.id || "");
            if (!id || vistos.has(id)) continue;

            vistos.add(id);
            resultado.push({ ...ocorrencia, __fila: fila });
        }
    }

    return resultado;
}

async function montarChamadoMapaEnriquecido(ocorrencia) {
    const contratoId = String(ocorrencia?.contrato || "").trim();
    const dadosContrato = contratoId ? await buscarDadosContrato(contratoId).catch(() => ({})) : {};
    const localizacao = await resolverLocalizacaoChamadoMapa(contratoId, dadosContrato);
    const dataAbertura = parseDataHoraOcorrenciaMapa(ocorrencia);
    const minutosAberto = dataAbertura ? Math.max(0, Math.round((Date.now() - dataAbertura.getTime()) / 60000)) : null;
    const responsavelBruto = String(ocorrencia?.responsavel || "").trim();

    return {
        id: String(ocorrencia.id),
        numero: ocorrencia.numero || "",
        fila: ocorrencia.__fila,
        status: ocorrencia.status || "",
        statusId: ocorrencia.status_id ?? null,
        cliente: ocorrencia.cliente || "",
        contrato: contratoId || null,
        motivo: extrairMotivoOcorrenciaMapa(ocorrencia),
        metodo: ocorrencia.metodo || "",
        dataAbertura: dataAbertura ? dataAbertura.toISOString() : null,
        dataAberturaDia: dataAbertura ? formatarDataLocalMapa(dataAbertura) : null,
        minutosAberto,
        tempoAbertoLabel: minutosAberto !== null ? formatarTempoAbertoLabelMapa(minutosAberto) : "-",
        prioridadeAntiga: minutosAberto !== null && minutosAberto >= MAPA_CHAMADOS_TEMPO_ANTIGO_MINUTOS,
        endereco: {
            logradouro: dadosContrato?.logradouro || "",
            numero: dadosContrato?.numero || "",
            bairro: dadosContrato?.bairro || "",
            cidade: dadosContrato?.cidade || "",
            uf: dadosContrato?.uf || "",
            cep: dadosContrato?.cep || "",
            enderecoCompleto: dadosContrato?.enderecoCompleto || ""
        },
        latitude: localizacao.latitude,
        longitude: localizacao.longitude,
        fonteLocalizacao: localizacao.fonteLocalizacao,
        statusLocalizacao: localizacao.statusLocalizacao,
        possuiLocalizacao: Boolean(validarCoordenadaRota(localizacao)),
        telefones: Array.isArray(dadosContrato?.telefones) ? dadosContrato.telefones : [],
        tecnicoResponsavelId: responsavelBruto || null,
        tecnicoResponsavelNome: responsavelBruto
            ? (obterTecnicoMapeadoAgenda(responsavelBruto, { incluirInstalacaoSemGps: true }) || responsavelBruto)
            : null
    };
}

async function obterChamadosMapaBase({ force = false } = {}) {
    if (!force && mapaChamadosBaseCache && mapaChamadosBaseCache.expiresAt > Date.now()) {
        return mapaChamadosBaseCache.data;
    }

    if (mapaChamadosBaseRequestEmAndamento) {
        return mapaChamadosBaseRequestEmAndamento;
    }

    const request = (async () => {
        const ocorrencias = await buscarChamadosSuporteAbertosSgp();
        const chamados = [];

        for (let i = 0; i < ocorrencias.length; i += MAPA_CHAMADOS_ENRIQUECIMENTO_CONCORRENCIA) {
            const lote = ocorrencias.slice(i, i + MAPA_CHAMADOS_ENRIQUECIMENTO_CONCORRENCIA);
            const resultadosLote = await Promise.all(lote.map(ocorrencia => montarChamadoMapaEnriquecido(ocorrencia)));
            chamados.push(...resultadosLote);
        }

        const dados = { chamados, geradoEm: new Date().toISOString() };

        mapaChamadosBaseCache = { data: dados, expiresAt: Date.now() + MAPA_CHAMADOS_BASE_CACHE_TTL_MS };

        return dados;
    })();

    mapaChamadosBaseRequestEmAndamento = request;

    try {
        return await request;
    } finally {
        mapaChamadosBaseRequestEmAndamento = null;
    }
}

let mapaChamadosAtualizacaoBackgroundIniciada = false;

function iniciarAtualizacaoBackgroundMapaChamados() {
    if (mapaChamadosAtualizacaoBackgroundIniciada) return;
    mapaChamadosAtualizacaoBackgroundIniciada = true;

    const atualizar = () => {
        obterChamadosMapaBase({ force: true }).catch(err => {
            console.error("⚠️ Falha ao atualizar cache em segundo plano do mapa de chamados:", err.message);
        });
    };

    atualizar();
    setInterval(atualizar, MAPA_CHAMADOS_REFRESH_BACKGROUND_MS);
}

function filtrarChamadosMapa(chamados, filtros) {
    const fila = String(filtros?.fila || "todos").trim().toUpperCase();
    const cidade = normalizarTextoRota(filtros?.cidade || "");
    const bairro = normalizarTextoRota(filtros?.bairro || "");
    const tempoAbertoMin = Number(filtros?.tempoAbertoMin) || 0;
    const busca = normalizarTextoRota(filtros?.busca || "");
    const apenasHoje = filtros?.hoje === "1" || filtros?.hoje === "true" || filtros?.hoje === true;
    const hojeStr = apenasHoje ? formatarDataLocalMapa(new Date()) : null;

    return chamados.filter(chamado => {
        if (fila !== "TODOS" && chamado.fila !== fila) return false;

        if (cidade && normalizarTextoRota(chamado.endereco.cidade) !== cidade) return false;
        if (bairro && normalizarTextoRota(chamado.endereco.bairro) !== bairro) return false;

        if (apenasHoje && chamado.dataAberturaDia !== hojeStr) return false;

        if (tempoAbertoMin > 0) {
            if (chamado.minutosAberto === null || chamado.minutosAberto < tempoAbertoMin) return false;
        }

        if (busca) {
            const alvo = normalizarTextoRota([
                chamado.cliente,
                chamado.contrato,
                chamado.numero,
                chamado.endereco.enderecoCompleto,
                chamado.endereco.bairro
            ].filter(Boolean).join(" "));

            if (!alvo.includes(busca)) return false;
        }

        return true;
    });
}

function calcularConcentracoesChamadosMapa(chamados) {
    const comLocalizacao = chamados.filter(chamado => chamado.possuiLocalizacao);
    const visitados = new Set();
    const concentracoes = [];
    let proximoId = 1;

    for (const chamado of comLocalizacao) {
        if (visitados.has(chamado.id)) continue;

        const grupo = [chamado];
        visitados.add(chamado.id);

        let expandiu = true;
        while (expandiu) {
            expandiu = false;

            for (const candidato of comLocalizacao) {
                if (visitados.has(candidato.id)) continue;

                const pertence = grupo.some(membro => calcularDistanciaKm(membro, candidato) <= MAPA_CHAMADOS_CONCENTRACAO_RAIO_KM);
                if (pertence) {
                    grupo.push(candidato);
                    visitados.add(candidato.id);
                    expandiu = true;
                }
            }
        }

        if (grupo.length >= MAPA_CHAMADOS_CONCENTRACAO_MIN_CHAMADOS) {
            const centroLatitude = grupo.reduce((soma, item) => soma + item.latitude, 0) / grupo.length;
            const centroLongitude = grupo.reduce((soma, item) => soma + item.longitude, 0) / grupo.length;
            const centro = { latitude: centroLatitude, longitude: centroLongitude };
            const raioKm = Math.max(...grupo.map(item => calcularDistanciaKm(centro, item)), 0);
            const bairrosContagem = grupo.reduce((acc, item) => {
                const chave = item.endereco.bairro || "Bairro não informado";
                acc[chave] = (acc[chave] || 0) + 1;
                return acc;
            }, {});
            const bairroPrincipal = Object.entries(bairrosContagem).sort((a, b) => b[1] - a[1])[0]?.[0] || "";
            const datas = grupo.map(item => item.dataAbertura).filter(Boolean).sort();

            concentracoes.push({
                id: proximoId++,
                bairro: bairroPrincipal,
                centro,
                raioMetros: Math.round(raioKm * 1000),
                totalChamados: grupo.length,
                n1: grupo.filter(item => item.fila === "N1").length,
                n2: grupo.filter(item => item.fila === "N2").length,
                st: grupo.filter(item => item.fila === "ST").length,
                primeiroChamado: datas[0] || null,
                ultimoChamado: datas[datas.length - 1] || null,
                chamados: grupo.map(item => ({
                    id: item.id,
                    contrato: item.contrato,
                    cliente: item.cliente,
                    fila: item.fila,
                    motivo: item.motivo
                }))
            });
        }
    }

    return concentracoes.sort((a, b) => b.totalChamados - a.totalChamados);
}

function montarIndicadoresMapaChamados(chamados, concentracoes) {
    const minutosAbertos = chamados.map(chamado => chamado.minutosAberto).filter(valor => Number.isFinite(valor));

    return {
        chamadosAbertos: chamados.length,
        n1: chamados.filter(chamado => chamado.fila === "N1").length,
        n2: chamados.filter(chamado => chamado.fila === "N2").length,
        st: chamados.filter(chamado => chamado.fila === "ST").length,
        regioesComConcentracao: concentracoes.length,
        chamadoMaisAntigoMinutos: minutosAbertos.length ? Math.max(...minutosAbertos) : null,
        chamadoMaisAntigoLabel: minutosAbertos.length ? formatarTempoAbertoLabelMapa(Math.max(...minutosAbertos)) : "-"
    };
}

function obterPermissoesMapaChamados(req) {
    const permissoes = obterPermissoesUsuario(req);
    const perfil = normalizarTextoRota(
        req?.headers?.["x-user-role"] || req?.headers?.["x-user-profile"] || req?.headers?.["x-perfil"] || ""
    );
    const podeVerTelefone = permissoes.has(MAPA_CHAMADOS_TELEFONE_PERMISSION)
        || ["admin", "administrador", "coordenador"].some(item => perfil.includes(item));

    return { podeVerTelefone };
}

function serializarChamadoMapaParaResposta(chamado, permissoes) {
    const { telefones, ...resto } = chamado;

    return {
        ...resto,
        telefone: permissoes.podeVerTelefone && telefones.length ? telefones[0] : null
    };
}

async function montarDadosMapaChamados(filtrosOrigem = {}, req = null) {
    iniciarAtualizacaoBackgroundMapaChamados();

    const force = filtrosOrigem?.force === "1" || filtrosOrigem?.force === "true" || filtrosOrigem?.force === true;
    const base = await obterChamadosMapaBase({ force });
    const permissoes = obterPermissoesMapaChamados(req);

    const chamadosFiltrados = filtrarChamadosMapa(base.chamados, filtrosOrigem);
    const concentracoes = calcularConcentracoesChamadosMapa(chamadosFiltrados);
    const indicadores = montarIndicadoresMapaChamados(chamadosFiltrados, concentracoes);
    const naoLocalizados = chamadosFiltrados.filter(chamado => !chamado.possuiLocalizacao);
    const cidades = [...new Set(base.chamados.map(chamado => chamado.endereco.cidade).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const bairros = [...new Set(base.chamados.map(chamado => chamado.endereco.bairro).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));

    return {
        geradoEm: base.geradoEm,
        atualizadoEm: new Date().toISOString(),
        filtros: {
            fila: String(filtrosOrigem?.fila || "todos"),
            cidade: filtrosOrigem?.cidade || "",
            bairro: filtrosOrigem?.bairro || "",
            tempoAbertoMin: Number(filtrosOrigem?.tempoAbertoMin) || 0,
            busca: filtrosOrigem?.busca || "",
            hoje: filtrosOrigem?.hoje === "1" || filtrosOrigem?.hoje === "true" || filtrosOrigem?.hoje === true
        },
        opcoesFiltro: { cidades, bairros },
        indicadores,
        chamados: chamadosFiltrados
            .filter(chamado => chamado.possuiLocalizacao)
            .map(chamado => serializarChamadoMapaParaResposta(chamado, permissoes)),
        concentracoes,
        naoLocalizados: naoLocalizados.map(chamado => ({
            id: chamado.id,
            numero: chamado.numero,
            cliente: chamado.cliente,
            contrato: chamado.contrato,
            fila: chamado.fila,
            motivo: chamado.motivo,
            enderecoCompleto: chamado.endereco.enderecoCompleto
        })),
        permissoes
    };
}

function ocorrenciaAtravessaPeriodoMassivo(ocorrencia, dataInicio, dataFim) {
    const cadastro = normalizarDataAusencia(ocorrencia?.data_cadastro);
    const finalizacao = normalizarDataAusencia(ocorrencia?.data_finalizacao);

    if (!cadastro) return false;
    return cadastro <= dataFim && (!finalizacao || finalizacao >= dataInicio);
}

function serializarOcorrenciaMassivo(ocorrencia) {
    return {
        id: ocorrencia.id || ocorrencia.ocorrencia || "",
        cliente: ocorrencia.cliente || "",
        contrato: ocorrencia.contrato || "",
        tipo: ocorrencia.tipo || "",
        tipoNormalizado: normalizarTipoMassivo(ocorrencia.tipo),
        local: obterLocalMassivoSgp(ocorrencia),
        pop: ocorrencia.pop || "",
        status: ocorrencia.status || "",
        statusId: ocorrencia.status_id ?? ocorrencia.statusId ?? "",
        dataCadastro: normalizarDataAusencia(ocorrencia.data_cadastro) || "",
        dataFinalizacao: normalizarDataAusencia(ocorrencia.data_finalizacao) || "",
        origens: Array.from(ocorrencia.__origens || [])
    };
}

function agruparMassivosHistorico(itens, campo) {
    return Object.entries(itens.reduce((acc, item) => {
        const chave = String(item[campo] || "Não informado").trim() || "Não informado";
        acc[chave] = (acc[chave] || 0) + 1;
        return acc;
    }, {}))
        .map(([nome, total]) => ({ nome, total }))
        .sort((a, b) => b.total - a.total || a.nome.localeCompare(b.nome, "pt-BR"));
}

async function montarHistoricoMassivos(periodo, opcoes = {}) {
    const incluirAtivos = Boolean(opcoes.incluirAtivos);
    const incluirCriados = Boolean(opcoes.incluirCriados);
    const cacheKey = `${periodo.dataInicio}|${periodo.dataFim}|ativos:${incluirAtivos}|criados:${incluirCriados}`;
    const cached = massivosHistoricoCache.get(cacheKey);

    if (!opcoes.force && cached && cached.expiresAt > Date.now()) {
        return cached.data;
    }

    const porId = new Map();
    const adicionar = (ocorrencias, origem) => {
        ocorrencias
            .filter(isOcorrenciaMassivoSgp)
            .forEach(ocorrencia => {
                const id = String(ocorrencia.id || ocorrencia.ocorrencia || `${ocorrencia.contrato}-${ocorrencia.tipo}-${ocorrencia.data_cadastro}`);
                const existente = porId.get(id) || ocorrencia;
                existente.__origens = new Set([...(existente.__origens || []), origem]);
                porId.set(id, { ...existente, ...ocorrencia, __origens: existente.__origens });
            });
    };

    if (incluirCriados) {
        const consultasCriados = await Promise.all(STATUS_OCORRENCIAS_HISTORICO_MASSIVO.map(async (status) => ({
            origem: "criado_no_periodo",
            ocorrencias: await buscarOcorrenciasSgp({
                status: Number(status),
                data_cadastro_inicio: periodo.dataInicio,
                data_cadastro_fim: periodo.dataFim
            })
        })));

        consultasCriados.forEach(({ ocorrencias, origem }) => adicionar(ocorrencias, origem));
    }

    const encerradasNoPeriodo = await buscarOcorrenciasSgp({
        status: 1,
        data_finalizacao_inicio: periodo.dataInicio,
        data_finalizacao_fim: periodo.dataFim
    });

    adicionar(encerradasNoPeriodo, "encerrado_no_periodo");

    if (incluirAtivos) {
        const consultasAtivas = await Promise.all(
            STATUS_OCORRENCIAS_ATIVAS_MASSIVO.map(async (status) => buscarOcorrenciasSgp({ status: Number(status) }))
        );

        consultasAtivas.forEach(ocorrencias => {
            adicionar(
                ocorrencias.filter(ocorrencia => ocorrenciaAtravessaPeriodoMassivo(ocorrencia, periodo.dataInicio, periodo.dataFim)),
                "ativo_no_periodo"
            );
        });
    }

    const massivos = Array.from(porId.values())
        .map(serializarOcorrenciaMassivo)
        .sort((a, b) => (
            String(b.dataFinalizacao || b.dataCadastro).localeCompare(String(a.dataFinalizacao || a.dataCadastro))
            || Number(b.id) - Number(a.id)
        ));

    const dados = {
        periodo,
        incluiAtivos: incluirAtivos,
        incluiCriados: incluirCriados,
        total: massivos.length,
        locais: agruparMassivosHistorico(massivos, "local"),
        tipos: agruparMassivosHistorico(massivos, "tipoNormalizado"),
        status: agruparMassivosHistorico(massivos, "status"),
        massivos,
        atualizadoEm: new Date().toISOString()
    };

    massivosHistoricoCache.set(cacheKey, {
        data: dados,
        expiresAt: Date.now() + MASSIVOS_HISTORICO_CACHE_TTL_MS
    });

    return dados;
}

function criarStoreResponsaveisVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function garantirArquivoResponsaveisServicos() {
    const dir = path.dirname(RESPONSAVEIS_SERVICOS_FILE);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(RESPONSAVEIS_SERVICOS_FILE)) {
        fs.writeFileSync(
            RESPONSAVEIS_SERVICOS_FILE,
            JSON.stringify(criarStoreResponsaveisVazio(), null, 2)
        );
    }
}

function obterListaTecnicosAgendaRastreame() {
    return [...obterTecnicosValidos()].filter(tecnico => !isTecnicoSemRastreamentoGps(tecnico));
}

function obterListaTecnicosAgendaRastreameCoordenadorRota(filtros = {}) {
    return [...obterTecnicosValidosCoordenadorRota(filtros)]
        .filter(tecnico => !isTecnicoSemRastreamentoGps(tecnico));
}

configurarTecnicosAgendaRastreame(obterListaTecnicosAgendaRastreame());

function normalizarStoreResponsaveis(store) {
    const registros = (Array.isArray(store?.items) ? store.items : [])
        .filter(item => item?.tecnico_id && item?.responsavel_nome)
        .map((item, index) => ({
            id: Number(item.id) || index + 1,
            tecnico_id: String(item.tecnico_id),
            responsavel_nome: String(item.responsavel_nome),
            updated_at: item.updated_at || null
        }));

    const maiorId = registros.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = registros.length === 0
        ? 1
        : Number.isInteger(store?.nextId) && store.nextId > maiorId
            ? store.nextId
            : maiorId + 1;

    return {
        nextId,
        items: registros
    };
}

function lerStoreResponsaveisServicos() {
    garantirArquivoResponsaveisServicos();
    const conteudo = fs.readFileSync(RESPONSAVEIS_SERVICOS_FILE, "utf8");
    return normalizarStoreResponsaveis(JSON.parse(conteudo || "{}"));
}

function salvarStoreResponsaveisServicos(store) {
    garantirArquivoResponsaveisServicos();
    const normalizado = normalizarStoreResponsaveis(store);
    const arquivoTemporario = `${RESPONSAVEIS_SERVICOS_FILE}.tmp`;

    fs.writeFileSync(arquivoTemporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(arquivoTemporario, RESPONSAVEIS_SERVICOS_FILE);

    return normalizado;
}

function listarResponsaveisServicos() {
    const tecnicosValidos = obterTecnicosValidos();
    return lerStoreResponsaveisServicos().items
        .filter(item => tecnicosValidos.has(item.tecnico_id))
        .sort((a, b) => a.tecnico_id.localeCompare(b.tecnico_id, "pt-BR"));
}

function emitirResponsaveisServicos() {
    io.emit("responsaveisServicosUpdate", listarResponsaveisServicos());
}

function validarTecnicoServico(tecnicoId) {
    return obterTecnicosValidos().has(tecnicoId);
}

function base64UrlAgendaTecnica(valor) {
    return Buffer.from(valor)
        .toString("base64")
        .replace(/=/g, "")
        .replace(/\+/g, "-")
        .replace(/\//g, "_");
}

function gerarJwtChatInterno(usuarioId) {
    const agora = Math.floor(Date.now() / 1000);
    const header = base64UrlAgendaTecnica(JSON.stringify({ alg: "HS256", typ: "JWT" }));
    const payload = base64UrlAgendaTecnica(JSON.stringify({
        id: usuarioId,
        iat: agora,
        exp: agora + 120
    }));
    const assinatura = crypto
        .createHmac("sha256", CHAT_INTERNO_JWT_SECRET)
        .update(`${header}.${payload}`)
        .digest("base64")
        .replace(/=/g, "")
        .replace(/\+/g, "-")
        .replace(/\//g, "_");

    return `${header}.${payload}.${assinatura}`;
}

function normalizarAvatarChatInterno(avatar) {
    const texto = String(avatar || "").trim();
    if (!texto) return "";
    if (/^data:image\//i.test(texto)) return texto;
    if (/^https?:\/\//i.test(texto)) return texto;
    if (texto.startsWith("/")) return `${CHAT_INTERNO_BASE_URL}${texto}`;
    return "";
}

function sanitizarUsuarioChatInterno(usuario) {
    const id = Number(usuario?.id);
    const nome = String(usuario?.nome || "").trim();

    if (!id || !nome) return null;

    return {
        id,
        nome,
        avatar: normalizarAvatarChatInterno(usuario.avatar),
        status: String(usuario.status || "").trim()
    };
}

async function fetchChatInternoJson(pathname, opcoes = {}) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), CHAT_INTERNO_TIMEOUT_MS);

    try {
        const resposta = await fetch(`${CHAT_INTERNO_BASE_URL}${pathname}`, {
            ...opcoes,
            signal: controller.signal
        });

        const texto = await resposta.text();
        let dados = null;

        try {
            dados = texto ? JSON.parse(texto) : null;
        } catch {
            dados = null;
        }

        return { resposta, dados };
    } finally {
        clearTimeout(timeout);
    }
}

async function descobrirUsuarioServicoChatInterno() {
    const empresaId = Number.isFinite(CHAT_INTERNO_EMPRESA_ID) && CHAT_INTERNO_EMPRESA_ID > 0
        ? CHAT_INTERNO_EMPRESA_ID
        : 1;
    const { resposta, dados } = await fetchChatInternoJson(`/api/usuarios/${encodeURIComponent(empresaId)}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "{}"
    });

    if (!resposta.ok || !Array.isArray(dados) || !dados.length) {
        return 0;
    }

    return Number(dados[0]?.id) || 0;
}

async function requisitarUsuariosChatInterno(usuarioId) {
    const token = gerarJwtChatInterno(usuarioId);
    const { resposta, dados } = await fetchChatInternoJson("/api/chat/usuarios", {
        headers: { Authorization: `Bearer ${token}` }
    });

    return { resposta, dados };
}

async function listarUsuariosChatInterno() {
    const agora = Date.now();

    if (
        chatInternoUsuariosCache.usuarios.length &&
        agora - chatInternoUsuariosCache.carregadoEm < CHAT_INTERNO_USUARIOS_CACHE_TTL_MS
    ) {
        return chatInternoUsuariosCache.usuarios;
    }

    let usuarioServicoId = chatInternoServiceUserIdCache;
    let resultado = usuarioServicoId ? await requisitarUsuariosChatInterno(usuarioServicoId) : null;

    if (!resultado || resultado.resposta.status === 401) {
        usuarioServicoId = await descobrirUsuarioServicoChatInterno();
        if (usuarioServicoId) {
            chatInternoServiceUserIdCache = usuarioServicoId;
            resultado = await requisitarUsuariosChatInterno(usuarioServicoId);
        }
    }

    if (!resultado?.resposta?.ok || !Array.isArray(resultado.dados)) {
        const status = resultado?.resposta?.status || 0;
        throw new Error(`Chat interno retornou status ${status || "indisponível"}.`);
    }

    const usuarios = resultado.dados
        .map(sanitizarUsuarioChatInterno)
        .filter(Boolean)
        .sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR"));

    chatInternoUsuariosCache = {
        carregadoEm: agora,
        usuarios
    };

    return usuarios;
}

function removerResponsavelServico(tecnicoId) {
    const store = lerStoreResponsaveisServicos();
    store.items = store.items.filter(item => item.tecnico_id !== tecnicoId);
    salvarStoreResponsaveisServicos(store);
    emitirResponsaveisServicos();
}

function criarStoreAusenciasVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function garantirArquivoAusenciasTecnicos() {
    const dir = path.dirname(AUSENCIAS_TECNICOS_FILE);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(AUSENCIAS_TECNICOS_FILE)) {
        fs.writeFileSync(
            AUSENCIAS_TECNICOS_FILE,
            JSON.stringify(criarStoreAusenciasVazio(), null, 2)
        );
    }
}

function normalizarDataAusencia(data) {
    const valor = String(data || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(valor)) return "";

    const date = new Date(`${valor}T00:00:00`);
    if (Number.isNaN(date.getTime())) return "";

    return valor;
}

function normalizarObservacaoAusencia(observacao) {
    return String(observacao || "")
        .trim()
        .slice(0, TAMANHO_MAXIMO_OBSERVACAO_AUSENCIA);
}

function normalizarStatusAusencia(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .trim();
}

function normalizarMotivoAusenciaTecnico(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .trim();
}

function isAusenciaCanceladaOuInativa(item = {}) {
    if (item.cancelled_at || item.canceled_at || item.deleted_at || item.removed_at) return true;
    if (item.active === false || item.ativo === false || item.cancelada === true || item.cancelled === true) return true;

    const status = normalizarStatusAusencia(item.status || item.situacao || item.state);
    return Boolean(status && ["cancelado", "cancelada", "canceled", "cancelled", "inativo", "inativa", "expirado", "expirada"].includes(status));
}

function isAusenciaDupladoTecnico(item = {}) {
    return normalizarMotivoAusenciaTecnico(item.absence_reason) === MOTIVO_AUSENCIA_DUPLADO;
}

function isAusenciaOperacionalTecnico(item = {}) {
    return !isAusenciaCanceladaOuInativa(item) && !isAusenciaDupladoTecnico(item);
}

function normalizarStoreAusencias(store) {
    const tecnicosValidos = obterTecnicosValidos();
    const porTecnicoAtivo = new Map();
    const registrosHistoricos = [];
    const idsUsados = new Set();
    const agora = new Date().toISOString();

    (Array.isArray(store?.items) ? store.items : []).forEach((item, index) => {
        const tecnicoId = String(item?.technician_id || "").trim();
        const absenceDate = normalizarDataAusencia(item?.absence_date);
        const absenceReason = normalizarMotivoAusenciaTecnico(item?.absence_reason);

        if (!tecnicoId || !tecnicosValidos.has(tecnicoId)) return;
        if (!absenceDate || !MOTIVOS_AUSENCIA_TECNICO.has(absenceReason)) return;

        let id = Number(item.id);
        if (!Number.isInteger(id) || id <= 0 || idsUsados.has(id)) {
            id = index + 1;
            while (idsUsados.has(id)) id += 1;
        }
        idsUsados.add(id);

        const inativa = isAusenciaCanceladaOuInativa(item);
        const deletedAt = inativa
            ? (item.deleted_at || item.removed_at || null)
            : null;

        const registro = {
            ...item,
            id,
            technician_id: tecnicoId,
            absence_date: absenceDate,
            absence_reason: absenceReason,
            absence_note: normalizarObservacaoAusencia(item.absence_note),
            active: !inativa,
            created_at: item.created_at || item.updated_at || agora,
            updated_at: item.updated_at || item.created_at || agora
        };

        if (deletedAt) {
            registro.deleted_at = deletedAt;
        } else {
            delete registro.deleted_at;
        }

        if (inativa) {
            registrosHistoricos.push(registro);
            return;
        }

        const registroAtual = porTecnicoAtivo.get(registro.technician_id);
        const novoTimestamp = Date.parse(registro.updated_at || registro.created_at || registro.absence_date);
        const atualTimestamp = registroAtual
            ? Date.parse(registroAtual.updated_at || registroAtual.created_at || registroAtual.absence_date)
            : Number.NEGATIVE_INFINITY;

        if (!registroAtual || (Number.isFinite(novoTimestamp) && novoTimestamp >= atualTimestamp)) {
            porTecnicoAtivo.set(registro.technician_id, registro);
        }
    });

    const registros = [...porTecnicoAtivo.values(), ...registrosHistoricos]
        .sort((a, b) => a.id - b.id);
    const maiorId = registros.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = registros.length === 0
        ? 1
        : Number.isInteger(store?.nextId) && store.nextId > maiorId
            ? store.nextId
            : maiorId + 1;

    return {
        nextId,
        items: registros
    };
}

function lerStoreAusenciasTecnicos() {
    garantirArquivoAusenciasTecnicos();
    const conteudo = fs.readFileSync(AUSENCIAS_TECNICOS_FILE, "utf8");
    return normalizarStoreAusencias(JSON.parse(conteudo || "{}"));
}

function salvarStoreAusenciasTecnicos(store) {
    garantirArquivoAusenciasTecnicos();
    const normalizado = normalizarStoreAusencias(store);
    const arquivoTemporario = `${AUSENCIAS_TECNICOS_FILE}.tmp`;

    fs.writeFileSync(arquivoTemporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(arquivoTemporario, AUSENCIAS_TECNICOS_FILE);

    return normalizado;
}

function listarAusenciasTecnicosAtivas() {
    return lerStoreAusenciasTecnicos().items
        .filter(item => !isAusenciaCanceladaOuInativa(item))
        .sort((a, b) => a.technician_id.localeCompare(b.technician_id, "pt-BR"));
}

function listarAusenciasTecnicosPorData() {
    // Compatibilidade com chamadas antigas: a data nao expira ausencias ativas.
    return listarAusenciasTecnicosAtivas();
}

function listarAusenciasOperacionaisTecnicosPorData() {
    return listarAusenciasTecnicosPorData().filter(isAusenciaOperacionalTecnico);
}

function emitirAusenciasTecnicos() {
    const absences = listarAusenciasTecnicosAtivas();

    io.emit("technicianAbsencesUpdate", {
        date: null,
        absences,
        ausencias: absences
    });
}

function obterUsuarioAcaoAusencia(req) {
    return String(
        req?.headers?.["x-user-name"]
        || req?.headers?.["x-usuario"]
        || req?.headers?.["x-user"]
        || "agenda-tecnicos"
    ).trim() || "agenda-tecnicos";
}

function montarPayloadAusencias(req, registroAtual = {}) {
    const technicianId = String(req.body?.technician_id || registroAtual.technician_id || "").trim();
    const absenceDate = normalizarDataAusencia(req.body?.absence_date || registroAtual.absence_date);
    const absenceReason = normalizarMotivoAusenciaTecnico(req.body?.absence_reason || registroAtual.absence_reason);
    const absenceNote = normalizarObservacaoAusencia(req.body?.absence_note);

    return {
        technicianId,
        absenceDate,
        absenceReason,
        absenceNote
    };
}

function validarPayloadAusencia({ technicianId, absenceDate, absenceReason }) {
    if (!validarTecnicoServico(technicianId)) {
        return "Técnico não encontrado.";
    }

    if (!absenceDate) {
        return "Data da ausência inválida.";
    }

    if (!MOTIVOS_AUSENCIA_TECNICO.has(absenceReason)) {
        return "Motivo da ausência inválido.";
    }

    return "";
}

function criarStoreCoresEquipeVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function garantirArquivoCoresEquipeTecnicos() {
    const dir = path.dirname(CORES_EQUIPE_TECNICOS_FILE);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(CORES_EQUIPE_TECNICOS_FILE)) {
        fs.writeFileSync(
            CORES_EQUIPE_TECNICOS_FILE,
            JSON.stringify(criarStoreCoresEquipeVazio(), null, 2)
        );
    }
}

function normalizarDataEquipe(data) {
    return normalizarDataAusencia(data);
}

function normalizarStoreCoresEquipe(store) {
    const tecnicosValidos = obterTecnicosValidos();
    const porTecnicoData = new Map();

    (Array.isArray(store?.items) ? store.items : []).forEach((item, index) => {
        const tecnicoId = String(item?.technician_id || "").trim();
        const teamDate = normalizarDataEquipe(item?.team_date);
        const teamColor = String(item?.team_color || "").trim();

        if (!tecnicoId || !tecnicosValidos.has(tecnicoId)) return;
        if (!teamDate || !CORES_EQUIPE_TECNICO.has(teamColor)) return;

        const registro = {
            id: Number(item.id) || index + 1,
            technician_id: tecnicoId,
            team_date: teamDate,
            team_color: teamColor,
            created_at: item.created_at || item.updated_at || new Date().toISOString(),
            updated_at: item.updated_at || item.created_at || new Date().toISOString()
        };

        porTecnicoData.set(`${registro.technician_id}|${registro.team_date}`, registro);
    });

    const registros = [...porTecnicoData.values()];
    const maiorId = registros.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = registros.length === 0
        ? 1
        : Number.isInteger(store?.nextId) && store.nextId > maiorId
            ? store.nextId
            : maiorId + 1;

    return {
        nextId,
        items: registros
    };
}

function lerStoreCoresEquipeTecnicos() {
    garantirArquivoCoresEquipeTecnicos();
    const conteudo = fs.readFileSync(CORES_EQUIPE_TECNICOS_FILE, "utf8");
    return normalizarStoreCoresEquipe(JSON.parse(conteudo || "{}"));
}

function salvarStoreCoresEquipeTecnicos(store) {
    garantirArquivoCoresEquipeTecnicos();
    const normalizado = normalizarStoreCoresEquipe(store);
    const arquivoTemporario = `${CORES_EQUIPE_TECNICOS_FILE}.tmp`;

    fs.writeFileSync(arquivoTemporario, JSON.stringify(normalizado, null, 2));
    fs.renameSync(arquivoTemporario, CORES_EQUIPE_TECNICOS_FILE);

    return normalizado;
}

function listarCoresEquipeTecnicosPorData(data) {
    const teamDate = normalizarDataEquipe(data);
    if (!teamDate) return [];

    return lerStoreCoresEquipeTecnicos().items
        .filter(item => item.team_date === teamDate)
        .sort((a, b) => a.technician_id.localeCompare(b.technician_id, "pt-BR"));
}

function emitirCoresEquipeTecnicos(data) {
    const teamDate = normalizarDataEquipe(data);
    if (!teamDate) return;

    io.emit("technicianTeamColorsUpdate", {
        date: teamDate,
        teamColors: listarCoresEquipeTecnicosPorData(teamDate)
    });
}

function montarPayloadCorEquipe(req, registroAtual = {}) {
    const technicianId = String(req.body?.technician_id || registroAtual.technician_id || "").trim();
    const teamDate = normalizarDataEquipe(req.body?.team_date || registroAtual.team_date);
    const teamColor = String(req.body?.team_color || registroAtual.team_color || "").trim();

    return {
        technicianId,
        teamDate,
        teamColor
    };
}

function validarPayloadCorEquipe({ technicianId, teamDate, teamColor }) {
    if (!validarTecnicoServico(technicianId)) {
        return "Técnico não encontrado.";
    }

    if (!teamDate) {
        return "Data da equipe inválida.";
    }

    if (!CORES_EQUIPE_TECNICO.has(teamColor)) {
        return "Cor da equipe inválida.";
    }

    return "";
}

function obterHojeBrasilBackend() {
    const partes = new Intl.DateTimeFormat("pt-BR", {
        timeZone: "America/Sao_Paulo",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
    }).formatToParts(new Date()).reduce((acc, parte) => {
        acc[parte.type] = parte.value;
        return acc;
    }, {});

    return `${partes.year}-${partes.month}-${partes.day}`;
}

function normalizarTextoRota(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .trim();
}

function normalizarChaveTecnicoRota(valor) {
    return normalizarTextoRota(valor).replace(/[^a-z0-9]/g, "");
}

function normalizarEscopoCoordenadorRota(origem = {}) {
    const valor = origem.escopo
        || origem.scope
        || origem.origem
        || origem.app
        || origem.aplicacao
        || "";
    const texto = normalizarTextoRota(valor).replace(/[_\s]+/g, "-");

    if (["instalacao", "instalacoes", "agenda-tecnicos-instalacao", "agenda-tecnicos-instalacoes"].includes(texto)) {
        return "instalacao";
    }

    return "";
}

function isEscopoInstalacaoCoordenadorRota(filtros = {}) {
    return normalizarEscopoCoordenadorRota(filtros) === "instalacao";
}

function normalizarTecnicoCoordenadorRota(tecnicoId) {
    const valor = String(tecnicoId || "").trim();
    if (!valor) return "";

    return TECNICOS_COORDENADOR_ROTA_INSTALACAO_ALIASES.get(normalizarChaveTecnicoRota(valor)) || valor;
}

function tecnicoPermitidoCoordenadorRota(tecnicoId, tecnicosValidos = obterTecnicosValidosCoordenadorRota()) {
    const tecnicoNormalizado = normalizarTecnicoCoordenadorRota(tecnicoId);
    return Boolean(tecnicoNormalizado && tecnicosValidos.has(tecnicoNormalizado));
}

function isMotivoCriticoRota(motivo) {
    const texto = normalizarTextoRota(motivo);
    return MOTIVOS_CRITICOS_ROTA.some(motivoCritico => texto.includes(motivoCritico));
}

function isStatusEncerradoRota(status) {
    const texto = normalizarTextoRota(status);
    return texto.includes("encerrada")
        || texto.includes("encerrado")
        || texto.includes("concluido")
        || texto.includes("concluida")
        || texto.includes("finalizado")
        || texto.includes("finalizada")
        || texto.includes("realizado")
        || texto.includes("realizada")
        || texto.includes("fechado")
        || texto.includes("fechada")
        || texto.includes("baixado")
        || texto.includes("baixada")
        || texto.includes("completed")
        || texto.includes("done");
}

function isStatusEmExecucaoRota(status) {
    const texto = normalizarTextoRota(status);
    const statusNumerico = Number(status);

    return statusNumerico === 2
        || texto.includes("execucao")
        || texto.includes("em execucao")
        || texto.includes("andamento")
        || texto.includes("em andamento")
        || texto.includes("running")
        || texto.includes("checkin")
        || texto.includes("check-in");
}

function getStatusAgendaTecnicaNumeroBackend(chamado = {}) {
    const statusOriginal = chamado?.status;
    const texto = normalizarTextoRota(statusOriginal);

    if (isStatusEncerradoRota(statusOriginal)) return 1;

    if (isStatusEmExecucaoRota(statusOriginal)) {
        return 2;
    }

    if (
        texto.includes("aberta")
        || texto.includes("aberto")
        || texto.includes("open")
        || texto.includes("aguardando")
        || texto.includes("pendente")
    ) {
        return 0;
    }

    const statusNumerico = Number(statusOriginal);
    if (Number.isInteger(statusNumerico) && statusNumerico >= 0 && statusNumerico <= 4) {
        return statusNumerico;
    }

    return 3;
}

function calcularTotaisStatusAgendaTecnicaBackend(agenda = []) {
    const lista = Array.isArray(agenda) ? agenda : [];
    const chamadosAbertos = lista.filter(chamado => getStatusAgendaTecnicaNumeroBackend(chamado) === 0).length;
    const chamadosEmAndamento = lista.filter(chamado => getStatusAgendaTecnicaNumeroBackend(chamado) === 2).length;

    return {
        chamadosAgenda: lista.length,
        chamadosAbertos,
        chamadosEmAndamento,
        chamadosAtivos: chamadosAbertos + chamadosEmAndamento
    };
}

function normalizarFiltrosCoordenadorRota(origem = {}) {
    const data = normalizarDataAusencia(origem.data) || obterHojeBrasilBackend();

    return {
        data,
        regiao: String(origem.regiao || "").trim(),
        tecnicoId: String(origem.tecnicoId || origem.tecnico_id || "").trim(),
        status: String(origem.status || "").trim(),
        motivo: String(origem.motivo || "").trim(),
        escopo: normalizarEscopoCoordenadorRota(origem)
    };
}

function parseCoordenadasRota(valor) {
    if (!valor) return null;

    if (typeof valor === "object") {
        const latitudeOrigem = valor.latitude
            ?? valor.lat
            ?? valor.Latitude
            ?? valor.LAT
            ?? valor.coordenada_latitude
            ?? valor.geo_latitude
            ?? valor.gps_latitude;
        const longitudeOrigem = valor.longitude
            ?? valor.lng
            ?? valor.lon
            ?? valor.Longitude
            ?? valor.LON
            ?? valor.coordenada_longitude
            ?? valor.geo_longitude
            ?? valor.gps_longitude;

        if (latitudeOrigem === null || latitudeOrigem === undefined || latitudeOrigem === "") return null;
        if (longitudeOrigem === null || longitudeOrigem === undefined || longitudeOrigem === "") return null;

        const latitude = Number(latitudeOrigem);
        const longitude = Number(longitudeOrigem);

        if (Number.isFinite(latitude) && Number.isFinite(longitude)) {
            return validarCoordenadaRota({ latitude, longitude });
        }
    }

    const texto = String(valor);
    const numeros = texto.match(/[-+]?\d{1,3}(?:[\.,]\d+)/g);

    if (!numeros || numeros.length < 2) return null;

    const latitude = Number(numeros[0].replace(",", "."));
    const longitude = Number(numeros[1].replace(",", "."));

    return validarCoordenadaRota({ latitude, longitude });
}

function validarCoordenadaRota(ponto) {
    if (!ponto) return null;
    if (ponto.latitude === null || ponto.latitude === undefined || ponto.latitude === "") return null;
    if (ponto.longitude === null || ponto.longitude === undefined || ponto.longitude === "") return null;

    const latitude = Number(ponto.latitude);
    const longitude = Number(ponto.longitude);

    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) return null;
    if (Math.abs(latitude) > 90 || Math.abs(longitude) > 180) return null;
    if (latitude === 0 && longitude === 0) return null;

    return { latitude, longitude };
}

function hashTextoRota(texto) {
    return String(texto || "").split("").reduce((hash, char) => {
        return ((hash << 5) - hash + char.charCodeAt(0)) | 0;
    }, 0);
}

function obterBaseRegiaoRota(regiao) {
    const chave = String(regiao || "").toUpperCase();
    return COORDENADAS_REGIOES_ROTA[chave] || COORDENADAS_REGIOES_ROTA.GERAL;
}

function getTecnicoColor(tecnicoId, index = null) {
    const nome = String(tecnicoId || "").trim();
    if (!nome) return "#64748b";

    if (CORES_ROTA_TECNICOS_FIXAS[nome]) return CORES_ROTA_TECNICOS_FIXAS[nome];

    const tecnicosOrdenados = [...obterTecnicosValidos({ incluirInstalacaoSemGps: true })]
        .sort((a, b) => a.localeCompare(b, "pt-BR"));
    const indice = Number.isInteger(index) && index >= 0
        ? index
        : Math.max(0, tecnicosOrdenados.findIndex(tecnico => tecnico === nome));

    return CORES_ROTA_TECNICOS[indice % CORES_ROTA_TECNICOS.length] || "#64748b";
}

function criarCoordenadaFallbackRota(regiao, seed) {
    const base = obterBaseRegiaoRota(regiao);
    const hash = Math.abs(hashTextoRota(`${regiao}:${seed}`));
    const deslocamentoLat = ((hash % 17) - 8) * 0.0017;
    const deslocamentoLng = (((Math.floor(hash / 17)) % 17) - 8) * 0.0017;

    return {
        latitude: Number((base.latitude + deslocamentoLat).toFixed(6)),
        longitude: Number((base.longitude + deslocamentoLng).toFixed(6))
    };
}

function detectarRegiaoRota(item = {}) {
    const texto = normalizarTextoRota([
        item.regiao,
        item.pop,
        item.bairro,
        item.endereco,
        item.cliente
    ].filter(Boolean).join(" "));

    if (texto.includes("iracema")) return "IRACEMA";
    if (texto.includes("francisco morato") || texto.includes("morato") || texto.includes("fmo")) return "FMO";
    if (texto.includes("franco da rocha") || texto.includes("franco") || texto.includes("fco")) return "FCO";
    if (texto.includes("caieiras") || texto.includes("cai")) return "CAI";
    if (texto.includes("laranjeiras") || texto.includes("laranjeias") || texto.includes("lrj")) return "LRJ";

    return "GERAL";
}

function calcularDistanciaKm(pontoA, pontoB) {
    const origem = validarCoordenadaRota(pontoA);
    const destino = validarCoordenadaRota(pontoB);

    if (!origem || !destino) return 0;

    const raioTerraKm = 6371;
    const grausParaRad = graus => graus * Math.PI / 180;
    const dLat = grausParaRad(destino.latitude - origem.latitude);
    const dLng = grausParaRad(destino.longitude - origem.longitude);
    const lat1 = grausParaRad(origem.latitude);
    const lat2 = grausParaRad(destino.latitude);
    const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) ** 2;
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return Number((raioTerraKm * c).toFixed(2));
}

function estimarTempoRotaMin(distanciaKm, quantidadeChamados = 0) {
    const tempoDeslocamento = (Number(distanciaKm) || 0) / VELOCIDADE_MEDIA_ROTA_KMH * 60;
    return Math.max(0, Math.round(tempoDeslocamento + (quantidadeChamados * MINUTOS_ATENDIMENTO_ROTA)));
}

function formatarKmRota(valor) {
    return `${Number(valor || 0).toFixed(1).replace(".", ",")} km`;
}

function formatarMinutosRota(valor) {
    return `${Math.max(0, Math.round(Number(valor) || 0))} minutos`;
}

function obterMinutosHoraSobrecarga(hora, fallback = null) {
    const partes = String(hora || "").match(/^(\d{1,2}):(\d{2})/);
    if (!partes) return fallback;

    const horas = Number(partes[1]);
    const minutos = Number(partes[2]);

    if (!Number.isInteger(horas) || !Number.isInteger(minutos)) return fallback;
    if (horas < 0 || horas > 24 || minutos < 0 || minutos > 59) return fallback;
    if (horas === 24 && minutos !== 0) return fallback;

    return (horas * 60) + minutos;
}

function obterMinutosAgoraBrasilBackend() {
    const partes = new Intl.DateTimeFormat("pt-BR", {
        timeZone: "America/Sao_Paulo",
        hour: "2-digit",
        minute: "2-digit",
        hour12: false
    }).formatToParts(new Date()).reduce((acc, parte) => {
        acc[parte.type] = parte.value;
        return acc;
    }, {});

    return (Number(partes.hour || 0) * 60) + Number(partes.minute || 0);
}

function normalizarTurnoSobrecarga(horario = null) {
    const inicio = obterMinutosHoraSobrecarga(horario?.startTime || TURNO_PADRAO_SOBRECARGA.startTime, 8 * 60);
    let fim = obterMinutosHoraSobrecarga(horario?.endTime || TURNO_PADRAO_SOBRECARGA.endTime, 18 * 60);

    if (fim <= inicio) fim += 24 * 60;

    return {
        inicio,
        fim,
        inicioTexto: horario?.startTime || TURNO_PADRAO_SOBRECARGA.startTime,
        fimTexto: horario?.endTime || TURNO_PADRAO_SOBRECARGA.endTime,
        nome: horario?.shiftName || TURNO_PADRAO_SOBRECARGA.shiftName,
        usandoPadrao: !horario
    };
}

function subtrairIntervaloSobrecarga(intervalos, bloqueioInicio, bloqueioFim) {
    if (!Number.isFinite(bloqueioInicio) || !Number.isFinite(bloqueioFim) || bloqueioFim <= bloqueioInicio) {
        return intervalos;
    }

    return intervalos.flatMap(intervalo => {
        if (bloqueioFim <= intervalo.inicio || bloqueioInicio >= intervalo.fim) return [intervalo];

        return [
            { inicio: intervalo.inicio, fim: Math.max(intervalo.inicio, bloqueioInicio) },
            { inicio: Math.min(intervalo.fim, bloqueioFim), fim: intervalo.fim }
        ].filter(item => item.fim > item.inicio);
    });
}

function montarIntervalosDisponiveisSobrecarga({ data, horario, almocos = [], ausente = false }) {
    const turno = normalizarTurnoSobrecarga(horario);
    if (ausente) return { turno, intervalos: [], disponibilidadeMin: 0 };

    let inicio = turno.inicio;
    const hoje = obterHojeBrasilBackend();

    if (data === hoje) {
        inicio = Math.max(inicio, obterMinutosAgoraBrasilBackend());
    }

    let intervalos = inicio < turno.fim
        ? [{ inicio, fim: turno.fim }]
        : [];

    almocos.forEach(almoco => {
        let inicioAlmoco = obterMinutosHoraSobrecarga(almoco.horaInicio || almoco.hora_inicio, null);
        let fimAlmoco = obterMinutosHoraSobrecarga(almoco.horaFim || almoco.hora_fim, null);

        if (!Number.isFinite(inicioAlmoco) || !Number.isFinite(fimAlmoco)) return;
        if (fimAlmoco <= inicioAlmoco) fimAlmoco += 24 * 60;

        intervalos = subtrairIntervaloSobrecarga(intervalos, inicioAlmoco, fimAlmoco);
    });

    const disponibilidadeMin = intervalos.reduce((total, intervalo) => total + Math.max(0, intervalo.fim - intervalo.inicio), 0);
    return { turno, intervalos, disponibilidadeMin };
}

function estimarAtendimentoChamadoSobrecarga(chamado) {
    const texto = normalizarTextoRota(`${chamado?.motivo || ""} ${chamado?.status || ""}`);

    if (texto.includes("instal")) return 120;
    if (texto.includes("mudanca") || texto.includes("mudança")) return 90;
    if (texto.includes("fibra") || texto.includes("cabo") || texto.includes("drop") || texto.includes("romp")) return 70;
    if (texto.includes("offline") || texto.includes("sem sinal")) return 50;
    if (texto.includes("retirada") || texto.includes("recolh")) return 40;
    if (texto.includes("roteador") || texto.includes("wifi") || texto.includes("wi-fi")) return 35;

    return MINUTOS_ATENDIMENTO_SOBRECARGA_PADRAO;
}

function obterDeslocamentoRotaSobrecarga(rota) {
    if (!rota) return 0;

    const tempoTotal = Number(rota.tempo_total_min) || 0;
    const paradas = Array.isArray(rota.itens) ? rota.itens.length : 0;
    const deslocamento = tempoTotal - (paradas * MINUTOS_ATENDIMENTO_ROTA);

    return Math.max(0, Math.round(deslocamento));
}

function calcularTerminoCargaSobrecarga(intervalos, cargaMin) {
    let restante = Math.max(0, Math.round(Number(cargaMin) || 0));
    if (!intervalos.length) return null;
    if (restante === 0) return intervalos[0].inicio;

    for (const intervalo of intervalos) {
        const capacidade = Math.max(0, intervalo.fim - intervalo.inicio);
        if (restante <= capacidade) {
            return intervalo.inicio + restante;
        }
        restante -= capacidade;
    }

    return intervalos[intervalos.length - 1].fim + restante;
}

function formatarHorarioSobrecarga(minutos) {
    if (!Number.isFinite(minutos)) return "";

    const total = Math.max(0, Math.round(minutos));
    const dias = Math.floor(total / (24 * 60));
    const minutosDia = total % (24 * 60);
    const horas = Math.floor(minutosDia / 60);
    const mins = minutosDia % 60;
    const horario = `${String(horas).padStart(2, "0")}:${String(mins).padStart(2, "0")}`;

    return dias ? `+${dias}d ${horario}` : horario;
}

function classificarSobrecargaTecnico({ ausente, cargaMin, disponibilidadeMin, ocupacaoPercent, excedenteMin, chamadosPendentes }) {
    if (ausente && chamadosPendentes > 0) return { nivel: "critico", label: "Ausente c/ chamados" };
    if (ausente) return { nivel: "ausente", label: "Ausente" };
    if (disponibilidadeMin <= 0 && cargaMin > 0) return { nivel: "critico", label: "Sem tempo" };
    if (cargaMin <= 0) return { nivel: "normal", label: "Normal" };
    if (ocupacaoPercent >= 115 || excedenteMin > 30) return { nivel: "critico", label: "Crítico" };
    if (ocupacaoPercent >= 95 || excedenteMin > 0) return { nivel: "sobrecarregado", label: "Sobrecarregado" };
    if (ocupacaoPercent >= 75) return { nivel: "atencao", label: "Atenção" };

    return { nivel: "normal", label: "Normal" };
}

function montarSobrecargaTecnico({ tecnico, chamados, rota, horario, almocos, data }) {
    const chamadosPendentes = chamados.filter(chamado => !isStatusEncerradoRota(chamado.status));
    const ausente = tecnico.status === "ausente";
    const disponibilidade = montarIntervalosDisponiveisSobrecarga({ data, horario, almocos, ausente });
    const atendimentoMin = chamadosPendentes.reduce((total, chamado) => total + estimarAtendimentoChamadoSobrecarga(chamado), 0);
    const deslocamentoMin = obterDeslocamentoRotaSobrecarga(rota);
    const chamadosSemLocalizacao = chamadosPendentes.filter(chamado => !chamadoTemLocalizacaoRota(chamado)).length;
    const chamadosCriticos = chamadosPendentes.filter(chamado => chamado.prioridade === "critica").length;
    const penalidadeMin = (chamadosSemLocalizacao * MINUTOS_PENALIDADE_SEM_LOCALIZACAO_SOBRECARGA)
        + (chamadosCriticos * MINUTOS_PENALIDADE_CRITICO_SOBRECARGA);
    const cargaMin = Math.max(0, Math.round(atendimentoMin + deslocamentoMin + penalidadeMin));
    const disponibilidadeMin = Math.max(0, Math.round(disponibilidade.disponibilidadeMin));
    const ocupacaoPercent = disponibilidadeMin > 0
        ? Math.round((cargaMin / disponibilidadeMin) * 100)
        : (cargaMin > 0 ? 999 : 0);
    const previsaoTerminoMin = calcularTerminoCargaSobrecarga(disponibilidade.intervalos, cargaMin);
    const excedenteMin = previsaoTerminoMin === null
        ? (cargaMin > 0 ? cargaMin : 0)
        : Math.max(0, Math.round(previsaoTerminoMin - disponibilidade.turno.fim));
    const classificacao = classificarSobrecargaTecnico({
        ausente,
        cargaMin,
        disponibilidadeMin,
        ocupacaoPercent,
        excedenteMin,
        chamadosPendentes: chamadosPendentes.length
    });
    const motivos = [];

    if (disponibilidadeMin <= 0 && cargaMin > 0) motivos.push("Sem tempo disponível no turno");
    if (ocupacaoPercent >= 95 && ocupacaoPercent < 999) motivos.push(`Ocupação em ${ocupacaoPercent}%`);
    if (excedenteMin > 0) motivos.push(`Passa ${formatarMinutosRota(excedenteMin)} do fim do turno`);
    if (chamadosCriticos > 0) motivos.push(`${chamadosCriticos} chamado(s) crítico(s)`);
    if (chamadosSemLocalizacao > 0) motivos.push(`${chamadosSemLocalizacao} chamado(s) sem localização confiável`);
    if (disponibilidade.turno.usandoPadrao) motivos.push("Usando horário padrão");

    return {
        ...classificacao,
        ocupacaoPercent,
        cargaMin,
        disponibilidadeMin,
        atendimentoMin,
        deslocamentoMin,
        penalidadeMin,
        excedenteMin,
        previsaoTermino: previsaoTerminoMin === null ? "" : formatarHorarioSobrecarga(previsaoTerminoMin),
        chamadosPendentes: chamadosPendentes.length,
        chamadosSemLocalizacao,
        chamadosCriticos,
        turnoInicio: disponibilidade.turno.inicioTexto,
        turnoFim: disponibilidade.turno.fimTexto,
        turnoNome: disponibilidade.turno.nome,
        almocoMin: (almocos || []).reduce((total, almoco) => {
            let inicio = obterMinutosHoraSobrecarga(almoco.horaInicio || almoco.hora_inicio, null);
            let fim = obterMinutosHoraSobrecarga(almoco.horaFim || almoco.hora_fim, null);
            if (!Number.isFinite(inicio) || !Number.isFinite(fim)) return total;
            if (fim <= inicio) fim += 24 * 60;
            return total + Math.max(0, fim - inicio);
        }, 0),
        motivos
    };
}

async function aplicarSobrecargaTecnicosCoordenadorRota({ data, tecnicos, chamados, rotas, tecnicosValidos }) {
    let horariosPorTecnico = new Map();
    let almocosDia = [];

    try {
        horariosPorTecnico = await obterHorariosEfetivosPorTecnicoData(data, tecnicosValidos);
    } catch (err) {
        console.warn("Não foi possível carregar horários para calcular sobrecarga:", err.message);
    }

    try {
        almocosDia = await listarAlmocosAgenda(data, tecnicosValidos);
    } catch (err) {
        console.warn("Não foi possível carregar almoços para calcular sobrecarga:", err.message);
    }

    const chamadosPorTecnico = new Map();
    const rotasPorTecnico = new Map((rotas || []).map(rota => [String(rota.tecnico_id), rota]));
    const almocosPorTecnico = new Map();

    chamados.forEach(chamado => {
        const tecnicoId = String(chamado.tecnicoAtualId || "");
        if (!chamadosPorTecnico.has(tecnicoId)) chamadosPorTecnico.set(tecnicoId, []);
        chamadosPorTecnico.get(tecnicoId).push(chamado);
    });

    almocosDia.forEach(almoco => {
        const tecnicoId = String(almoco.tecnicoId || almoco.tecnico_id || "");
        if (!almocosPorTecnico.has(tecnicoId)) almocosPorTecnico.set(tecnicoId, []);
        almocosPorTecnico.get(tecnicoId).push(almoco);
    });

    const resumo = {
        normal: 0,
        atencao: 0,
        sobrecarregado: 0,
        critico: 0,
        ausente: 0,
        avaliados: tecnicos.length,
        emRisco: 0,
        maiorOcupacaoPercent: 0,
        maiorCargaTecnico: null
    };

    tecnicos.forEach(tecnico => {
        const sobrecarga = montarSobrecargaTecnico({
            tecnico,
            chamados: chamadosPorTecnico.get(String(tecnico.id)) || [],
            rota: rotasPorTecnico.get(String(tecnico.id)),
            horario: horariosPorTecnico.get(String(tecnico.id)),
            almocos: almocosPorTecnico.get(String(tecnico.id)) || [],
            data
        });

        tecnico.sobrecarga = sobrecarga;

        if (Object.prototype.hasOwnProperty.call(resumo, sobrecarga.nivel)) {
            resumo[sobrecarga.nivel] += 1;
        }
        if (["sobrecarregado", "critico"].includes(sobrecarga.nivel)) {
            resumo.emRisco += 1;
        }
        if (sobrecarga.ocupacaoPercent > resumo.maiorOcupacaoPercent && sobrecarga.ocupacaoPercent < 999) {
            resumo.maiorOcupacaoPercent = sobrecarga.ocupacaoPercent;
            resumo.maiorCargaTecnico = tecnico.nome;
        }
    });

    return resumo;
}

async function montarSobrecargaAgendaTecnica(agenda, data) {
    const tecnicosValidos = obterTecnicosValidosAgendaTecnica();
    const ausencias = new Set(listarAusenciasOperacionaisTecnicosPorData(data).map(item => item.technician_id));
    const chamados = montarChamadosCoordenadorRota(agenda, data)
        .filter(chamado => tecnicosValidos.has(chamado.tecnicoAtualId));
    const tecnicosSobrecarga = [...tecnicosValidos]
        .sort((a, b) => a.localeCompare(b, "pt-BR"))
        .map(nome => ({
            id: nome,
            nome,
            status: ausencias.has(nome) ? "ausente" : "disponivel"
        }));
    const resumo = await aplicarSobrecargaTecnicosCoordenadorRota({
        data,
        tecnicos: tecnicosSobrecarga,
        chamados,
        rotas: [],
        tecnicosValidos
    });
    const totaisStatus = calcularTotaisStatusAgendaTecnicaBackend(agenda);
    const chamadosSimultaneos = montarResumoChamadosSimultaneosRota(chamados, data);

    return {
        data,
        resumo: {
            ...resumo,
            ...totaisStatus,
            tecnicosComChamadosSimultaneos: chamadosSimultaneos.totalTecnicos,
            chamadosSimultaneos: chamadosSimultaneos.totalTecnicos
        },
        tecnicos: Object.fromEntries(tecnicosSobrecarga.map(tecnico => [tecnico.id, tecnico.sobrecarga])),
        chamadosSimultaneos
    };
}

function garantirArquivoJsonRota(arquivo, conteudoInicial) {
    const dir = path.dirname(arquivo);

    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
    }

    if (!fs.existsSync(arquivo)) {
        fs.writeFileSync(arquivo, JSON.stringify(conteudoInicial, null, 2));
    }
}

function lerJsonRota(arquivo, conteudoInicial) {
    garantirArquivoJsonRota(arquivo, conteudoInicial);

    try {
        return JSON.parse(fs.readFileSync(arquivo, "utf8") || "{}");
    } catch (err) {
        console.error(`Erro ao ler ${path.basename(arquivo)}:`, err.message);
        return conteudoInicial;
    }
}

function salvarJsonRota(arquivo, conteudo) {
    garantirArquivoJsonRota(arquivo, conteudo);
    const arquivoTemporario = `${arquivo}.tmp`;

    fs.writeFileSync(arquivoTemporario, JSON.stringify(conteudo, null, 2));
    fs.renameSync(arquivoTemporario, arquivo);
}

function criarStoreSugestoesRotaVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function normalizarStoreSugestoesRota(store) {
    const items = (Array.isArray(store?.items) ? store.items : []).map((item, index) => ({
        id: Number(item.id) || index + 1,
        chave: String(item.chave || ""),
        data_sugestao: normalizarDataAusencia(item.data_sugestao) || obterHojeBrasilBackend(),
        tipo_sugestao: String(item.tipo_sugestao || "mover_chamado"),
        chamado_id: item.chamado_id ?? null,
        cliente: String(item.cliente || ""),
        regiao: String(item.regiao || "GERAL"),
        status_chamado: String(item.status_chamado || ""),
        motivo_chamado: String(item.motivo_chamado || ""),
        tecnico_atual_id: item.tecnico_atual_id ?? null,
        tecnico_atual_nome: String(item.tecnico_atual_nome || item.tecnico_atual_id || ""),
        tecnico_sugerido_id: item.tecnico_sugerido_id ?? null,
        tecnico_sugerido_nome: String(item.tecnico_sugerido_nome || item.tecnico_sugerido_id || ""),
        motivo: String(item.motivo || ""),
        mensagem: String(item.mensagem || item.motivo || ""),
        detalhe: String(item.detalhe || ""),
        prioridade: String(item.prioridade || "normal"),
        economia_tempo_min: Math.max(0, Math.round(Number(item.economia_tempo_min) || 0)),
        economia_km: Number(Number(item.economia_km || 0).toFixed(2)),
        status: ["pendente", "aplicada", "rejeitada"].includes(item.status) ? item.status : "pendente",
        ordem_sugerida: Array.isArray(item.ordem_sugerida) ? item.ordem_sugerida : [],
        created_at: item.created_at || new Date().toISOString(),
        updated_at: item.updated_at || item.created_at || new Date().toISOString(),
        applied_at: item.applied_at || null,
        rejected_at: item.rejected_at || null,
        usuario_acao: item.usuario_acao || null
    }));
    const maiorId = items.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;

    return { nextId, items };
}

function lerStoreSugestoesRota() {
    return normalizarStoreSugestoesRota(lerJsonRota(COORDENADOR_ROTA_SUGESTOES_FILE, criarStoreSugestoesRotaVazio()));
}

function salvarStoreSugestoesRota(store) {
    const normalizado = normalizarStoreSugestoesRota(store);
    salvarJsonRota(COORDENADOR_ROTA_SUGESTOES_FILE, normalizado);
    return normalizado;
}

function criarStoreLogsRotaVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function registrarLogCoordenadorRota(acao, detalhes = {}, req = null) {
    const store = lerJsonRota(COORDENADOR_ROTA_LOG_FILE, criarStoreLogsRotaVazio());
    const items = Array.isArray(store.items) ? store.items : [];
    const maiorId = items.reduce((maior, item) => Math.max(maior, Number(item.id) || 0), 0);
    const id = Number.isInteger(store.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;

    items.push({
        id,
        acao,
        detalhes,
        usuario: obterUsuarioAcaoRota(req),
        ip: req?.ip || null,
        created_at: new Date().toISOString()
    });

    salvarJsonRota(COORDENADOR_ROTA_LOG_FILE, {
        nextId: id + 1,
        items: items.slice(-1000)
    });
}

function obterUsuarioAcaoRota(req) {
    return String(req?.headers?.["x-user-name"] || req?.headers?.["x-usuario"] || "agenda-tecnicos").trim();
}

function criarStoreChamadosSimultaneosVazio() {
    return {
        nextId: 1,
        principais: [],
        historico: []
    };
}

function normalizarStoreChamadosSimultaneos(store) {
    const principais = (Array.isArray(store?.principais) ? store.principais : []).map((item, index) => ({
        id: Number(item.id) || index + 1,
        data: normalizarDataAusencia(item.data) || obterHojeBrasilBackend(),
        tecnico_id: String(item.tecnico_id || item.tecnicoId || item.tecnico || "").trim(),
        chamado_id: String(item.chamado_id || item.chamadoId || item.chamado || "").trim(),
        motivo: String(item.motivo || "Definição manual pelo coordenador").trim(),
        usuario: String(item.usuario || "agenda-tecnicos").trim(),
        created_at: item.created_at || new Date().toISOString(),
        updated_at: item.updated_at || item.created_at || new Date().toISOString()
    })).filter(item => item.data && item.tecnico_id && item.chamado_id);

    const historico = (Array.isArray(store?.historico) ? store.historico : []).map((item, index) => ({
        id: Number(item.id) || index + 1,
        data: normalizarDataAusencia(item.data) || obterHojeBrasilBackend(),
        tecnico_id: String(item.tecnico_id || item.tecnicoId || item.tecnico || "").trim(),
        chamado_id: String(item.chamado_id || item.chamadoId || item.chamado || "").trim(),
        chamados_envolvidos: Array.isArray(item.chamados_envolvidos) ? item.chamados_envolvidos.map(String) : [],
        motivo: String(item.motivo || "Definição manual pelo coordenador").trim(),
        usuario: String(item.usuario || "agenda-tecnicos").trim(),
        created_at: item.created_at || new Date().toISOString()
    })).filter(item => item.data && item.tecnico_id && item.chamado_id);

    const maiorId = [...principais, ...historico].reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;

    return { nextId, principais, historico };
}

function lerStoreChamadosSimultaneos() {
    return normalizarStoreChamadosSimultaneos(lerJsonRota(AGENDA_CHAMADOS_SIMULTANEOS_FILE, criarStoreChamadosSimultaneosVazio()));
}

function salvarStoreChamadosSimultaneos(store) {
    const normalizado = normalizarStoreChamadosSimultaneos(store);
    salvarJsonRota(AGENDA_CHAMADOS_SIMULTANEOS_FILE, normalizado);
    return normalizado;
}

function obterChaveChamadoPrincipalSimultaneo(data, tecnicoId) {
    return `${normalizarDataAusencia(data) || obterHojeBrasilBackend()}|${String(tecnicoId || "").trim()}`;
}

function obterMapaChamadosPrincipaisSimultaneos(store = lerStoreChamadosSimultaneos()) {
    const mapa = new Map();

    (store.principais || []).forEach(item => {
        mapa.set(obterChaveChamadoPrincipalSimultaneo(item.data, item.tecnico_id), item);
    });

    return mapa;
}

function obterChamadoPrincipalManualSimultaneo({ data, tecnicoId, store = lerStoreChamadosSimultaneos() }) {
    return obterMapaChamadosPrincipaisSimultaneos(store).get(obterChaveChamadoPrincipalSimultaneo(data, tecnicoId)) || null;
}

function registrarChamadoPrincipalSimultaneo({ data, tecnicoId, chamadoId, motivo, chamadosEnvolvidos = [], req = null }) {
    const store = lerStoreChamadosSimultaneos();
    const dataNormalizada = normalizarDataAusencia(data) || obterHojeBrasilBackend();
    const tecnico = String(tecnicoId || "").trim();
    const chamado = String(chamadoId || "").trim();
    const usuario = obterUsuarioAcaoRota(req);
    const agora = new Date().toISOString();

    if (!tecnico || !chamado) {
        const erro = new Error("Técnico e chamado são obrigatórios.");
        erro.statusCode = 400;
        throw erro;
    }

    const chave = obterChaveChamadoPrincipalSimultaneo(dataNormalizada, tecnico);
    let principal = store.principais.find(item => obterChaveChamadoPrincipalSimultaneo(item.data, item.tecnico_id) === chave);

    if (!principal) {
        principal = {
            id: store.nextId,
            data: dataNormalizada,
            tecnico_id: tecnico,
            chamado_id: chamado,
            motivo: String(motivo || "Definição manual pelo coordenador").trim(),
            usuario,
            created_at: agora,
            updated_at: agora
        };
        store.nextId += 1;
        store.principais.push(principal);
    } else {
        principal.chamado_id = chamado;
        principal.motivo = String(motivo || principal.motivo || "Definição manual pelo coordenador").trim();
        principal.usuario = usuario;
        principal.updated_at = agora;
    }

    store.historico.push({
        id: store.nextId,
        data: dataNormalizada,
        tecnico_id: tecnico,
        chamado_id: chamado,
        chamados_envolvidos: [...new Set(chamadosEnvolvidos.map(String).filter(Boolean))],
        motivo: principal.motivo,
        usuario,
        created_at: agora
    });
    store.nextId += 1;

    salvarStoreChamadosSimultaneos({
        ...store,
        historico: store.historico.slice(-1000)
    });

    registrarLogCoordenadorRota("definir_chamado_principal_simultaneo", {
        data: dataNormalizada,
        tecnico_id: tecnico,
        chamado_id: chamado,
        chamados_envolvidos: chamadosEnvolvidos,
        motivo: principal.motivo
    }, req);

    return principal;
}

function obterInicioExecucaoChamadoRota(chamado = {}) {
    return chamado.inicioExecucao
        || chamado.inicio_execucao
        || chamado.atendimentoIniciadoEm
        || chamado.atendimento_iniciado_em
        || chamado.iniciadoEm
        || chamado.iniciado_em
        || chamado.hora_inicio_execucao
        || chamado.data_inicio_execucao
        || chamado.checkin
        || chamado.Checkin
        || "";
}

function obterDataInicioExecucaoChamadoRota(chamado = {}, data) {
    return normalizarDataHoraReferenciaRota(obterInicioExecucaoChamadoRota(chamado), data, chamado.horario || chamado.hora || "");
}

function formatarHorarioInicioExecucaoChamadoRota(chamado = {}, data) {
    const dataInicio = new Date(obterDataInicioExecucaoChamadoRota(chamado, data));
    if (Number.isNaN(dataInicio.getTime())) return obterInicioExecucaoChamadoRota(chamado) || "";

    return dataInicio.toLocaleTimeString("pt-BR", {
        hour: "2-digit",
        minute: "2-digit",
        timeZone: "America/Sao_Paulo"
    });
}

function formatarTempoExecucaoChamadoRota(chamado = {}, data) {
    const dataInicio = new Date(obterDataInicioExecucaoChamadoRota(chamado, data));
    if (Number.isNaN(dataInicio.getTime())) return "";

    const minutos = Math.max(0, Math.floor((Date.now() - dataInicio.getTime()) / 60000));
    return formatarMinutosRota(minutos);
}

function ordenarChamadosExecucaoSimultaneosRota(chamados = [], data) {
    return [...chamados].sort((a, b) => {
        const dataA = Date.parse(obterDataInicioExecucaoChamadoRota(a, data)) || 0;
        const dataB = Date.parse(obterDataInicioExecucaoChamadoRota(b, data)) || 0;
        if (dataA !== dataB) return dataB - dataA;
        return obterMinutosHorarioRota(b.horario || b.hora) - obterMinutosHorarioRota(a.horario || a.hora);
    });
}

function escolherChamadoPrincipalExecucaoRota({ tecnicoId, chamadosExecucao = [], data, store = lerStoreChamadosSimultaneos() }) {
    const chamadosOrdenados = ordenarChamadosExecucaoSimultaneosRota(chamadosExecucao, data);
    const manual = obterChamadoPrincipalManualSimultaneo({ data, tecnicoId, store });
    const manualValido = manual
        ? chamadosOrdenados.find(chamado => String(chamado.id) === String(manual.chamado_id))
        : null;

    return {
        chamado: manualValido || chamadosOrdenados[0] || null,
        principalManual: Boolean(manualValido),
        correcaoManual: manualValido ? manual : null
    };
}

function montarDetalheChamadoSimultaneoRota(chamado, data, principalId) {
    const inicioExecucaoMs = Date.parse(obterDataInicioExecucaoChamadoRota(chamado, data)) || 0;

    return {
        id: String(chamado.id || ""),
        chamado_id: String(chamado.id || ""),
        numero: String(chamado.id || ""),
        cliente: chamado.cliente || "Cliente sem nome",
        bairro: chamado.bairro || "",
        endereco: chamado.enderecoCompleto || chamado.endereco || [chamado.logradouro, chamado.numero, chamado.bairro].filter(Boolean).join(", "),
        tipoAtendimento: chamado.motivo || "",
        horarioInicio: formatarHorarioInicioExecucaoChamadoRota(chamado, data),
        inicioExecucao: obterInicioExecucaoChamadoRota(chamado),
        inicioExecucaoMs,
        tempoExecucao: formatarTempoExecucaoChamadoRota(chamado, data),
        responsavelStatus: chamado.tecnicoResponsavelSgpId || chamado.tecnico_responsavel_sgp_id || chamado.tecnicoAtualNome || chamado.tecnicoAtualId || "-",
        status: chamado.status || "Em execução",
        hora: chamado.horario || "",
        pop: chamado.pop || "",
        latitude: chamado.latitude ?? null,
        longitude: chamado.longitude ?? null,
        principal: String(chamado.id || "") === String(principalId || "")
    };
}

function montarResumoChamadosSimultaneosRota(chamados = [], data) {
    const store = lerStoreChamadosSimultaneos();
    const chamadosPorTecnico = new Map();

    chamados
        .filter(chamado => isStatusEmExecucaoRota(chamado.status) && !isStatusEncerradoRota(chamado.status))
        .forEach(chamado => {
            const tecnicoId = String(chamado.tecnicoAtualId || chamado.tecnicoDesignadoId || "").trim();
            if (!tecnicoId) return;
            if (!chamadosPorTecnico.has(tecnicoId)) chamadosPorTecnico.set(tecnicoId, []);
            chamadosPorTecnico.get(tecnicoId).push(chamado);
        });

    const tecnicos = [...chamadosPorTecnico.entries()]
        .map(([tecnicoId, chamadosTecnico]) => {
            if (chamadosTecnico.length < 2) return null;

            const { chamado, principalManual, correcaoManual } = escolherChamadoPrincipalExecucaoRota({
                tecnicoId,
                chamadosExecucao: chamadosTecnico,
                data,
                store
            });
            const principalId = chamado?.id || "";

            return {
                tecnico_id: tecnicoId,
                tecnicoId,
                nome: tecnicoId,
                total: chamadosTecnico.length,
                chamadoPrincipalId: String(principalId || ""),
                chamadoPrincipalInicio: chamado ? formatarHorarioInicioExecucaoChamadoRota(chamado, data) : "",
                principalManual,
                motivoPrincipalManual: correcaoManual?.motivo || "",
                usuarioPrincipalManual: correcaoManual?.usuario || "",
                atualizadoPrincipalManual: correcaoManual?.updated_at || null,
                chamados: ordenarChamadosExecucaoSimultaneosRota(chamadosTecnico, data)
                    .map(item => montarDetalheChamadoSimultaneoRota(item, data, principalId))
            };
        })
        .filter(Boolean)
        .sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR"));

    return {
        data: normalizarDataAusencia(data) || obterHojeBrasilBackend(),
        totalTecnicos: tecnicos.length,
        totalChamados: tecnicos.reduce((total, item) => total + item.total, 0),
        tecnicos
    };
}

function listarLocalizacoesTecnicosRota() {
    const store = lerJsonRota(TECNICOS_LOCALIZACAO_FILE, { nextId: 1, items: [] });
    const localizacoes = Array.isArray(store?.items) ? store.items : [];
    const tecnicosValidos = obterTecnicosValidos();
    const porTecnico = new Map();

    localizacoes.forEach((item, index) => {
        const tecnicoId = String(item?.tecnico_id || item?.technician_id || "").trim();
        if (!tecnicosValidos.has(tecnicoId)) return;

        const coordenada = validarCoordenadaRota({
            latitude: item.latitude,
            longitude: item.longitude
        });
        if (!coordenada) return;

        const registro = {
            id: Number(item.id) || index + 1,
            tecnico_id: tecnicoId,
            chamado_referencia_id: item.chamado_referencia_id || item.chamadoReferenciaId || null,
            latitude: coordenada.latitude,
            longitude: coordenada.longitude,
            accuracy: Number(item.accuracy) || null,
            origem: String(item.origem || item.fonte_localizacao || "manual"),
            fonte_localizacao: String(item.fonte_localizacao || item.origem || "manual"),
            descricao: String(item.descricao || item.localizacaoDescricao || ""),
            data_referencia: item.data_referencia || item.dataReferencia || item.created_at || new Date().toISOString(),
            created_at: item.created_at || new Date().toISOString(),
            updated_at: item.updated_at || item.created_at || new Date().toISOString()
        };
        const atual = porTecnico.get(tecnicoId);
        const novoTimestamp = Date.parse(registro.data_referencia || registro.updated_at || registro.created_at);
        const atualTimestamp = atual ? Date.parse(atual.data_referencia || atual.updated_at || atual.created_at) : Number.NEGATIVE_INFINITY;

        if (!atual || novoTimestamp >= atualTimestamp) {
            porTecnico.set(tecnicoId, registro);
        }
    });

    return porTecnico;
}

function salvarLocalizacaoTecnicoRota({ tecnicoId, chamado, coordenada, fonteLocalizacao, descricao, dataReferencia }) {
    if (!tecnicoId || !coordenada) return null;

    const store = lerJsonRota(TECNICOS_LOCALIZACAO_FILE, { nextId: 1, items: [] });
    const items = Array.isArray(store.items) ? store.items : [];
    const agora = new Date().toISOString();
    const chamadoReferenciaId = chamado?.id ? String(chamado.id) : null;
    const existente = items.find(item => (
        String(item.tecnico_id || "") === String(tecnicoId)
        && String(item.chamado_referencia_id || "") === String(chamadoReferenciaId || "")
        && String(item.fonte_localizacao || item.origem || "") === String(fonteLocalizacao)
    ));

    if (existente) {
        existente.latitude = coordenada.latitude;
        existente.longitude = coordenada.longitude;
        existente.descricao = descricao;
        existente.data_referencia = dataReferencia || agora;
        existente.updated_at = agora;
    } else {
        const maiorId = items.reduce((maior, item) => Math.max(maior, Number(item.id) || 0), 0);
        items.push({
            id: Number.isInteger(store.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1,
            tecnico_id: String(tecnicoId),
            chamado_referencia_id: chamadoReferenciaId,
            latitude: coordenada.latitude,
            longitude: coordenada.longitude,
            fonte_localizacao: fonteLocalizacao,
            origem: fonteLocalizacao,
            descricao,
            data_referencia: dataReferencia || agora,
            created_at: agora,
            updated_at: agora
        });
    }

    const maiorIdFinal = items.reduce((maior, item) => Math.max(maior, Number(item.id) || 0), 0);
    salvarJsonRota(TECNICOS_LOCALIZACAO_FILE, {
        nextId: maiorIdFinal + 1,
        items: items.slice(-1000)
    });

    return items[items.length - 1] || null;
}

function criarStoreLocalizacoesChamadosVazio() {
    return {
        nextId: 1,
        items: []
    };
}

function montarChaveLocalizacaoChamado(chamadoId, enderecoCompleto = "") {
    return `${String(chamadoId || "").trim()}|${normalizarTextoRota(enderecoCompleto)}`;
}

function normalizarStatusLocalizacaoRota(status) {
    const valor = String(status || "").trim();
    return STATUS_LOCALIZACAO_ROTA.has(valor) ? valor : "sem_localizacao";
}

function normalizarFonteLocalizacaoRota(fonte, fallback = "desconhecida") {
    const valor = String(fonte || "").trim();
    const normalizado = normalizarTextoRota(valor)
        .replace(/[^a-z0-9_]+/g, "_")
        .replace(/^_+|_+$/g, "");
    const mapa = {
        cache_manual: "corrigida_manual",
        confirmada_manual: "confirmada",
        sgp_reprocessado: "sgp",
        sgp_sincronizado: "sgp",
        backend: "desconhecida",
        cache: "desconhecida",
        reprocessamento_manual: "desconhecida"
    };
    const fonteNormalizada = mapa[normalizado] || normalizado || fallback;

    return FONTES_LOCALIZACAO_ROTA.has(fonteNormalizada) ? fonteNormalizada : fallback;
}

function isFonteSgpLocalizacaoRota(item) {
    const fonte = normalizarFonteLocalizacaoRota(item?.fonte_localizacao || item?.origem, "");
    const origem = normalizarTextoRota(item?.origem || "");
    return fonte === "sgp" || origem.includes("sgp");
}

function isLocalizacaoManualProtegidaRota(item) {
    if (!item) return false;

    const status = normalizarStatusLocalizacaoRota(item.status_localizacao);
    const fonte = normalizarFonteLocalizacaoRota(item.fonte_localizacao || item.origem, "");
    const origem = normalizarTextoRota(item.origem || "");

    if (status === "corrigida_manual") return true;
    if (["manual", "corrigida_manual"].includes(fonte)) return true;
    if (fonte === "confirmada" && !isFonteSgpLocalizacaoRota(item)) return true;
    if (["manual", "corrigida_manual", "confirmada_manual"].some(valor => origem.includes(valor))) return true;

    return false;
}

function coordenadasDiferentesRota(coordenadaA, coordenadaB, tolerancia = 0.000001) {
    const a = validarCoordenadaRota(coordenadaA);
    const b = validarCoordenadaRota(coordenadaB);

    if (!a || !b) return Boolean(a || b);

    return Math.abs(a.latitude - b.latitude) > tolerancia
        || Math.abs(a.longitude - b.longitude) > tolerancia;
}

function normalizarStoreLocalizacoesChamados(store) {
    const items = (Array.isArray(store?.items) ? store.items : [])
        .map((item, index) => {
            const latitude = Number(item.latitude);
            const longitude = Number(item.longitude);
            const sgpLatitude = Number(item.sgp_latitude ?? item.sgpLatitude);
            const sgpLongitude = Number(item.sgp_longitude ?? item.sgpLongitude);
            const status = normalizarStatusLocalizacaoRota(item.status_localizacao);
            const origem = String(item.origem || item.fonte_localizacao || "cache");

            return {
                id: Number(item.id) || index + 1,
                chamado_id: String(item.chamado_id || ""),
                chave: String(item.chave || montarChaveLocalizacaoChamado(item.chamado_id, item.enderecoCompleto || item.endereco_completo || "")),
                enderecoCompleto: String(item.enderecoCompleto || item.endereco_completo || ""),
                logradouro: String(item.logradouro || ""),
                numero: String(item.numero || ""),
                bairro: String(item.bairro || ""),
                cidade: String(item.cidade || ""),
                uf: String(item.uf || ""),
                cep: String(item.cep || ""),
                latitude: Number.isFinite(latitude) ? latitude : null,
                longitude: Number.isFinite(longitude) ? longitude : null,
                status_localizacao: status,
                fonte_localizacao: normalizarFonteLocalizacaoRota(item.fonte_localizacao || origem),
                origem,
                sgp_latitude: Number.isFinite(sgpLatitude) ? sgpLatitude : null,
                sgp_longitude: Number.isFinite(sgpLongitude) ? sgpLongitude : null,
                sgp_atualizado_em: item.sgp_atualizado_em || item.sgpAtualizadoEm || null,
                ultima_sincronizacao_sgp: item.ultima_sincronizacao_sgp || item.ultimaSincronizacaoSgp || null,
                observacao: String(item.observacao || ""),
                created_at: item.created_at || new Date().toISOString(),
                updated_at: item.updated_at || item.created_at || new Date().toISOString()
            };
        })
        .filter(item => item.chamado_id || item.chave);
    const maiorId = items.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;

    return { nextId, items };
}

function lerStoreLocalizacoesChamados() {
    return normalizarStoreLocalizacoesChamados(lerJsonRota(CHAMADOS_LOCALIZACAO_FILE, criarStoreLocalizacoesChamadosVazio()));
}

function salvarStoreLocalizacoesChamados(store) {
    const normalizado = normalizarStoreLocalizacoesChamados(store);
    salvarJsonRota(CHAMADOS_LOCALIZACAO_FILE, normalizado);
    return normalizado;
}

function listarLocalizacoesChamadosPorChave(store = lerStoreLocalizacoesChamados()) {
    const porChave = new Map();

    store.items.forEach(item => {
        const chave = item.chave || montarChaveLocalizacaoChamado(item.chamado_id, item.enderecoCompleto);
        const atual = porChave.get(chave);
        const novoTimestamp = Date.parse(item.updated_at || item.created_at);
        const atualTimestamp = atual ? Date.parse(atual.updated_at || atual.created_at) : Number.NEGATIVE_INFINITY;

        if (!atual || novoTimestamp >= atualTimestamp) {
            porChave.set(chave, item);
        }
    });

    return porChave;
}

function obterLocalizacaoCacheChamado(localizacoesCache, chamadoId, enderecoCompleto = "") {
    const chave = montarChaveLocalizacaoChamado(chamadoId, enderecoCompleto);
    const porChave = localizacoesCache.get(chave);
    const candidatos = [...localizacoesCache.values()]
        .filter(item => String(item.chamado_id) === String(chamadoId));

    if (porChave && !candidatos.includes(porChave)) {
        candidatos.push(porChave);
    }

    if (!candidatos.length) return null;

    return candidatos
        .sort((a, b) => {
            const score = item => {
                const coordenada = validarCoordenadaRota(item);
                const temEndereco = Boolean(String(item.enderecoCompleto || "").trim());
                const status = normalizarStatusLocalizacaoRota(item.status_localizacao);
                const fonte = normalizarFonteLocalizacaoRota(item.fonte_localizacao || item.origem, "");
                return (coordenada ? 1000 : 0)
                    + (status === "corrigida_manual" ? 300 : 0)
                    + (status === "confirmada" ? 200 : 0)
                    + (fonte === "sgp" ? 120 : 0)
                    + (temEndereco ? 80 : 0);
            };
            const scoreDiff = score(b) - score(a);
            if (scoreDiff !== 0) return scoreDiff;

            return Date.parse(b.updated_at || b.created_at || 0) - Date.parse(a.updated_at || a.created_at || 0);
        })[0] || null;
}

function salvarLocalizacaoChamadoCache({
    chamadoId,
    enderecoCompleto,
    coordenada,
    statusLocalizacao,
    origem,
    observacao,
    fonteLocalizacao,
    sgpCoordenada,
    sgpAtualizadoEm,
    ultimaSincronizacaoSgp,
    enderecoSgp,
    preservarCoordenadaAtual = false
}, opcoes = {}) {
    const store = opcoes.store || lerStoreLocalizacoesChamados();
    const chave = montarChaveLocalizacaoChamado(chamadoId, enderecoCompleto);
    const agora = new Date().toISOString();
    let item = store.items.find(registro => registro.chave === chave);

    if (!item) {
        item = {
            id: store.nextId,
            chamado_id: String(chamadoId || ""),
            chave,
            enderecoCompleto: enderecoCompleto || "",
            created_at: agora
        };
        store.nextId += 1;
        store.items.push(item);
    }

    if (!preservarCoordenadaAtual) {
        item.latitude = coordenada?.latitude ?? null;
        item.longitude = coordenada?.longitude ?? null;
    }

    if (enderecoSgp) {
        item.enderecoCompleto = enderecoSgp.enderecoCompleto || item.enderecoCompleto || enderecoCompleto || "";
        item.logradouro = enderecoSgp.logradouro || item.logradouro || "";
        item.numero = enderecoSgp.numero || item.numero || "";
        item.bairro = enderecoSgp.bairro || item.bairro || "";
        item.cidade = enderecoSgp.cidade || item.cidade || "";
        item.uf = enderecoSgp.uf || item.uf || "";
        item.cep = enderecoSgp.cep || item.cep || "";
    }
    item.status_localizacao = normalizarStatusLocalizacaoRota(statusLocalizacao);
    item.origem = origem || "backend";
    item.fonte_localizacao = normalizarFonteLocalizacaoRota(fonteLocalizacao || origem || item.fonte_localizacao);

    if (sgpCoordenada !== undefined) {
        const coordenadaSgp = validarCoordenadaRota(sgpCoordenada);
        item.sgp_latitude = coordenadaSgp?.latitude ?? null;
        item.sgp_longitude = coordenadaSgp?.longitude ?? null;
    }

    if (sgpAtualizadoEm !== undefined) {
        item.sgp_atualizado_em = sgpAtualizadoEm || null;
    }

    if (ultimaSincronizacaoSgp !== undefined) {
        item.ultima_sincronizacao_sgp = ultimaSincronizacaoSgp || null;
    }

    item.observacao = observacao || "";
    item.updated_at = agora;

    if (opcoes.salvar !== false) {
        salvarStoreLocalizacoesChamados(store);
    }

    return item;
}

function montarMetadataLocalizacaoChamado(item, fallback = {}) {
    const coordenadaSgp = validarCoordenadaRota({
        latitude: item?.sgp_latitude,
        longitude: item?.sgp_longitude
    });
    const coordenadaAtual = validarCoordenadaRota(item);

    return {
        status_localizacao: normalizarStatusLocalizacaoRota(item?.status_localizacao || fallback.status_localizacao),
        origemCoordenada: item?.origem || fallback.origemCoordenada || "desconhecida",
        fonteLocalizacao: normalizarFonteLocalizacaoRota(item?.fonte_localizacao || item?.origem || fallback.fonteLocalizacao),
        ultimaSincronizacaoSgp: item?.ultima_sincronizacao_sgp || fallback.ultimaSincronizacaoSgp || null,
        sgpLatitude: coordenadaSgp?.latitude ?? null,
        sgpLongitude: coordenadaSgp?.longitude ?? null,
        sgpAtualizadoEm: item?.sgp_atualizado_em || fallback.sgpAtualizadoEm || null,
        sgpPossuiCoordenadaDiferente: Boolean(coordenadaAtual && coordenadaSgp && coordenadasDiferentesRota(coordenadaAtual, coordenadaSgp)),
        observacaoLocalizacao: item?.observacao || fallback.observacaoLocalizacao || ""
    };
}

function obterLocalizacaoChamadoPadronizada({
    chamadoId,
    enderecoCompleto,
    localizacaoApi,
    localizacoesCache,
    localizacoesStore = null,
    onLocalizacaoCacheAlterada = null
}) {
    const chave = montarChaveLocalizacaoChamado(chamadoId, enderecoCompleto);
    const cached = obterLocalizacaoCacheChamado(localizacoesCache, chamadoId, enderecoCompleto);
    const coordenadaCache = validarCoordenadaRota(cached);
    const salvarCache = (dados) => {
        const item = salvarLocalizacaoChamadoCache(dados, {
            store: localizacoesStore || undefined,
            salvar: !localizacoesStore
        });
        localizacoesCache.set(item.chave || chave, item);
        if (localizacoesStore && typeof onLocalizacaoCacheAlterada === "function") {
            onLocalizacaoCacheAlterada();
        }
        return item;
    };

    if (cached && cached.status_localizacao === "corrigida_manual" && coordenadaCache) {
        return {
            latitude: coordenadaCache.latitude,
            longitude: coordenadaCache.longitude,
            ...montarMetadataLocalizacaoChamado(cached, {
                status_localizacao: "corrigida_manual",
                origemCoordenada: "cache_manual",
                fonteLocalizacao: "corrigida_manual"
            })
        };
    }

    if (cached && cached.status_localizacao === "confirmada" && coordenadaCache && !isFonteSgpLocalizacaoRota(cached)) {
        return {
            latitude: coordenadaCache.latitude,
            longitude: coordenadaCache.longitude,
            ...montarMetadataLocalizacaoChamado(cached, {
                status_localizacao: "confirmada",
                origemCoordenada: "cache"
            })
        };
    }

    const coordenadaApi = parseCoordenadasRota(localizacaoApi);
    if (coordenadaApi) {
        if (!cached || !isLocalizacaoManualProtegidaRota(cached)) {
            salvarCache({
                chamadoId,
                enderecoCompleto,
                coordenada: coordenadaApi,
                statusLocalizacao: "confirmada",
                origem: "sgp",
                fonteLocalizacao: "sgp",
                sgpCoordenada: coordenadaApi,
                sgpAtualizadoEm: new Date().toISOString(),
                observacao: "Coordenada recebida da API."
            });
        }

        const itemAtual = localizacoesCache.get(chave);
        return {
            latitude: itemAtual && isLocalizacaoManualProtegidaRota(itemAtual) && coordenadaCache ? coordenadaCache.latitude : coordenadaApi.latitude,
            longitude: itemAtual && isLocalizacaoManualProtegidaRota(itemAtual) && coordenadaCache ? coordenadaCache.longitude : coordenadaApi.longitude,
            ...montarMetadataLocalizacaoChamado(itemAtual, {
                status_localizacao: "confirmada",
                origemCoordenada: "sgp",
                fonteLocalizacao: "sgp",
                sgpLatitude: coordenadaApi.latitude,
                sgpLongitude: coordenadaApi.longitude
            })
        };
    }

    if (cached && coordenadaCache) {
        return {
            latitude: coordenadaCache.latitude,
            longitude: coordenadaCache.longitude,
            ...montarMetadataLocalizacaoChamado(cached, {
                status_localizacao: normalizarStatusLocalizacaoRota(cached.status_localizacao),
                origemCoordenada: cached.origem || "cache"
            })
        };
    }

    if (!cached) {
        salvarCache({
            chamadoId,
            enderecoCompleto,
            coordenada: null,
            statusLocalizacao: "sem_localizacao",
            origem: "backend",
            observacao: "Chamado sem latitude/longitude na API."
        });
    }

    return {
        latitude: null,
        longitude: null,
        ...montarMetadataLocalizacaoChamado(localizacoesCache.get(chave), {
            status_localizacao: "sem_localizacao",
            origemCoordenada: "sem_localizacao",
            fonteLocalizacao: "desconhecida"
        })
    };
}

function obterTransferenciasAplicadasRota(data) {
    const transferencias = new Map();

    lerStoreSugestoesRota().items
        .filter(item => item.status === "aplicada")
        .filter(item => item.data_sugestao === data)
        .filter(item => item.chamado_id && item.tecnico_sugerido_id)
        .forEach(item => {
            transferencias.set(String(item.chamado_id), String(item.tecnico_sugerido_id));
        });

    return transferencias;
}

function obterEnderecoCompletoChamado(os) {
    const enderecoPronto = String(os.enderecoCompleto || os.endereco || "").trim();
    if (enderecoPronto) return enderecoPronto;

    return [
        os.logradouro,
        os.numero,
        os.bairro,
        os.cidade,
        os.uf
    ].filter(Boolean).join(", ");
}

function inferirPartesEnderecoRota(os, regiao) {
    const partes = String(os.enderecoCompleto || os.endereco || "")
        .split(",")
        .map(parte => parte.trim())
        .filter(Boolean);
    const cidadeRegiao = obterBaseRegiaoRota(regiao).label;
    const numeroInferido = os.numero || (partes[1] && /\d/.test(partes[1]) ? partes[1] : "");
    const bairroInferido = os.bairro || partes.find((parte, index) => {
        const texto = normalizarTextoRota(parte);
        if (index === 0) return false;
        if (numeroInferido && parte === numeroInferido) return false;
        if (/^\d+\s*[a-z]?$/i.test(parte)) return false;
        if (/\d{5}-?\d{3}/.test(parte)) return false;
        if (texto === "sp" || texto.includes("sao paulo")) return false;
        if (texto === normalizarTextoRota(cidadeRegiao)) return false;
        return true;
    }) || "";

    return {
        logradouro: os.logradouro || partes[0] || "",
        numero: numeroInferido,
        bairro: bairroInferido,
        cidade: os.cidade || cidadeRegiao || "",
        uf: os.uf || "SP",
        cep: os.cep || (String(os.enderecoCompleto || os.endereco || "").match(/\d{5}-?\d{3}/)?.[0] || "")
    };
}

function obterChamadoIdCoordenadorRota(os, data, index = 0) {
    return String(os.id || os.idOrdemServico || os.idCliente || `${data}-${index + 1}`);
}

function obterTecnicoOriginalChamadoRota(os) {
    const tecnico = String(os.tecnico || "").trim();
    if (!tecnico) return "";
    if (obterTecnicosValidosCoordenadorRota().has(tecnico)) return tecnico;
    return tecnico.split(" ")[0] || tecnico;
}

function montarChamadosCoordenadorRota(agenda, data) {
    const transferenciasAplicadas = obterTransferenciasAplicadasRota(data);
    const localizacoesCache = listarLocalizacoesChamadosPorChave();

    return agenda.map((os, index) => {
        const chamadoId = obterChamadoIdCoordenadorRota(os, data, index);
        const tecnicoOriginal = obterTecnicoOriginalChamadoRota(os);
        const tecnicoAtualId = transferenciasAplicadas.get(chamadoId) || tecnicoOriginal;
        const regiao = detectarRegiaoRota(os);
        const enderecoApi = obterEnderecoCompletoChamado(os);
        const localizacaoCache = obterLocalizacaoCacheChamado(localizacoesCache, chamadoId, enderecoApi);
        const enderecoCompleto = enderecoApi || localizacaoCache?.enderecoCompleto || "";
        const osEndereco = {
            ...os,
            enderecoCompleto,
            endereco: enderecoCompleto || os.endereco || "",
            logradouro: os.logradouro || localizacaoCache?.logradouro || "",
            numero: os.numero || localizacaoCache?.numero || "",
            bairro: os.bairro || localizacaoCache?.bairro || "",
            cidade: os.cidade || localizacaoCache?.cidade || "",
            uf: os.uf || localizacaoCache?.uf || "",
            cep: os.cep || localizacaoCache?.cep || ""
        };
        const partesEndereco = inferirPartesEnderecoRota(osEndereco, regiao);
        const localizacao = obterLocalizacaoChamadoPadronizada({
            chamadoId,
            enderecoCompleto,
            localizacaoApi: os.localizacao,
            localizacoesCache
        });
        const motivoCritico = isMotivoCriticoRota(os.motivo);
        const prioridade = motivoCritico ? "critica" : "normal";
        const corTecnico = getTecnicoColor(tecnicoAtualId);

        return {
            id: chamadoId,
            cliente: os.cliente || "Cliente sem nome",
            motivo: os.motivo || "",
            enderecoCompleto,
            endereco: enderecoCompleto,
            logradouro: partesEndereco.logradouro,
            numero: partesEndereco.numero,
            bairro: partesEndereco.bairro,
            cidade: partesEndereco.cidade,
            uf: partesEndereco.uf,
            cep: partesEndereco.cep,
            pop: os.pop || "",
            regiao,
            status: os.status || "Outro",
            prioridade,
            tecnicoAtualId,
            tecnicoAtualNome: tecnicoAtualId,
            tecnicoDesignadoId: tecnicoAtualId,
            tecnicoDesignadoNome: tecnicoAtualId,
            tecnicoOriginalId: tecnicoOriginal,
            tecnicoOriginalNome: tecnicoOriginal,
            tecnicoResponsavelSgpId: os.tecnicoResponsavelSgpId || os.tecnico_responsavel_sgp_id || "",
            tecnico_responsavel_sgp_id: os.tecnicoResponsavelSgpId || os.tecnico_responsavel_sgp_id || "",
            corTecnico,
            horario: os.hora || "",
            data_finalizado: os.data_finalizado || "",
            checkin: os.Checkin || "",
            clienteId: os.idCliente || getContratoId(os) || "",
            contratoId: os.idCliente || getContratoId(os) || "",
            idOrdemServico: os.idOrdemServico || os.id || "",
            latitude: localizacao.latitude,
            longitude: localizacao.longitude,
            status_localizacao: localizacao.status_localizacao,
            statusLocalizacao: localizacao.status_localizacao,
            origemCoordenada: localizacao.origemCoordenada,
            fonteLocalizacao: localizacao.fonteLocalizacao,
            ultimaSincronizacaoSgp: localizacao.ultimaSincronizacaoSgp,
            sgpLatitude: localizacao.sgpLatitude,
            sgpLongitude: localizacao.sgpLongitude,
            sgpAtualizadoEm: localizacao.sgpAtualizadoEm,
            sgpPossuiCoordenadaDiferente: Boolean(localizacao.sgpPossuiCoordenadaDiferente),
            possuiConflitoLocalizacaoSgp: Boolean(localizacao.sgpPossuiCoordenadaDiferente && isLocalizacaoManualProtegidaRota({
                status_localizacao: localizacao.status_localizacao,
                fonte_localizacao: localizacao.fonteLocalizacao,
                origem: localizacao.origemCoordenada
            })),
            observacaoLocalizacao: localizacao.observacaoLocalizacao || "",
            motivoSemLocalizacao: !validarCoordenadaRota(localizacao)
                ? (localizacao.observacaoLocalizacao || "SGP não retornou latitude/longitude")
                : "",
            possuiLocalizacao: Boolean(validarCoordenadaRota(localizacao)),
            ordemAtendimento: 0
        };
    });
}

function filtrarChamadosCoordenadorRota(chamados, filtros) {
    const regiao = normalizarTextoRota(filtros.regiao);
    const tecnicoId = normalizarTextoRota(filtros.tecnicoId);
    const status = normalizarTextoRota(filtros.status);
    const motivo = normalizarTextoRota(filtros.motivo);

    return chamados.filter(chamado => {
        if (regiao && regiao !== "todos" && normalizarTextoRota(chamado.regiao) !== regiao) return false;
        if (tecnicoId && tecnicoId !== "todos" && normalizarTextoRota(chamado.tecnicoAtualId) !== tecnicoId) return false;
        if (status && status !== "todos" && normalizarTextoRota(chamado.status) !== status) return false;
        if (motivo && motivo !== "todos" && !normalizarTextoRota(chamado.motivo).includes(motivo)) return false;

        return true;
    });
}

function filtrarChamadosPorEscopoCoordenadorRota(chamados, filtros) {
    if (!isEscopoInstalacaoCoordenadorRota(filtros)) return chamados;

    const tecnicosValidos = obterTecnicosValidosCoordenadorRota(filtros);

    return chamados.filter(chamado => tecnicoPermitidoCoordenadorRota(chamado.tecnicoAtualId, tecnicosValidos)
        || tecnicoPermitidoCoordenadorRota(chamado.tecnicoDesignadoId, tecnicosValidos));
}

function obterRegiaoPrincipalChamados(chamados) {
    const contagem = new Map();

    chamados.forEach(chamado => {
        contagem.set(chamado.regiao, (contagem.get(chamado.regiao) || 0) + 1);
    });

    return [...contagem.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "GERAL";
}

function montarDataHoraRota(data, horario) {
    const partes = String(horario || "").match(/(\d{1,2}):(\d{2})/);
    if (!partes) return `${data}T08:00:00-03:00`;
    return `${data}T${partes[1].padStart(2, "0")}:${partes[2]}:00-03:00`;
}

function normalizarDataHoraReferenciaRota(valor, data, horarioFallback = "") {
    const texto = String(valor || "").trim();

    if (texto) {
        const br = texto.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
        if (br) {
            const [, dia, mes, ano, hora = "0", minuto = "0", segundo = "0"] = br;
            const dataBr = new Date(Number(ano), Number(mes) - 1, Number(dia), Number(hora), Number(minuto), Number(segundo));
            if (!Number.isNaN(dataBr.getTime())) return dataBr.toISOString();
        }

        const dataIso = new Date(texto);
        if (!Number.isNaN(dataIso.getTime())) return dataIso.toISOString();
    }

    return montarDataHoraRota(data, horarioFallback);
}

function obterDataReferenciaChamadoRota(chamado, data) {
    return normalizarDataHoraReferenciaRota(chamado?.data_finalizado || chamado?.checkin || "", data, chamado?.horario || "");
}

function obterDescricaoFonteLocalizacaoRota(fonteLocalizacao) {
    const fonte = String(fonteLocalizacao || "");
    const labels = {
        gps_rastreame: "RastreaMe / GPS do veículo",
        gps_real: "GPS real",
        manual: "Manual",
        chamado_em_execucao: "Chamado em execução",
        ultimo_chamado: "Localização baseada no último chamado",
        chamado_finalizado: "Último chamado finalizado",
        casa_tecnico: "Casa cadastrada do técnico",
        primeiro_chamado_do_dia: "Primeiro chamado do dia",
        pop_base: "POP / Base",
        desconhecida: "Desconhecida"
    };

    return labels[fonte] || fonte || "Desconhecida";
}

function fonteSalvaTemPrioridadeRota(localizacaoSalva) {
    const fonte = normalizarTextoRota(localizacaoSalva?.fonte_localizacao || localizacaoSalva?.origem);
    return fonte === "gps_real";
}

function fonteSalvaManualRota(localizacaoSalva) {
    const fonte = normalizarTextoRota(localizacaoSalva?.fonte_localizacao || localizacaoSalva?.origem);
    return fonte === "manual";
}

function montarLocalizacaoDesconhecidaRota() {
    return {
        latitude: null,
        longitude: null,
        fonteLocalizacao: "desconhecida",
        origemLocalizacao: "desconhecida",
        localizacaoDescricao: "Técnico sem localização válida para o dia.",
        ultimaAtualizacaoLocalizacao: null,
        chamadoReferenciaLocalizacaoId: null,
        chamadoReferenciaLocalizacaoCliente: null,
        statusPosicaoGps: "sem_gps",
        empresaRastreame: null,
        vinculoRastreameOrigem: null,
        veiculo: null
    };
}

function montarLocalizacaoTecnicoPorChamado({ chamado, data, fonteLocalizacao }) {
    const coordenada = validarCoordenadaRota(chamado);
    if (!coordenada) return montarLocalizacaoDesconhecidaRota();

    const finalizado = fonteLocalizacao === "chamado_finalizado";
    const ultimoChamado = fonteLocalizacao === "ultimo_chamado";
    const emExecucao = fonteLocalizacao === "chamado_em_execucao";
    const descricao = emExecucao
        ? `Chamado em execução: ${chamado.cliente}`
        : finalizado
        ? `Último chamado finalizado: ${chamado.cliente}`
        : ultimoChamado
            ? `Localização baseada no último chamado: ${chamado.cliente}`
            : `Primeiro chamado do dia: ${chamado.cliente}`;
    const dataReferencia = emExecucao || finalizado || ultimoChamado
        ? obterDataReferenciaChamadoRota(chamado, data)
        : normalizarDataHoraReferenciaRota("", data, chamado.horario || "");

    return {
        latitude: coordenada.latitude,
        longitude: coordenada.longitude,
        fonteLocalizacao,
        origemLocalizacao: fonteLocalizacao,
        localizacaoDescricao: descricao,
        ultimaAtualizacaoLocalizacao: dataReferencia,
        chamadoReferenciaLocalizacaoId: chamado.id,
        chamadoReferenciaLocalizacaoCliente: chamado.cliente,
        statusPosicaoGps: "sem_gps",
        empresaRastreame: null,
        vinculoRastreameOrigem: null,
        veiculo: null
    };
}

function ordenarChamadosPorReferenciaLocalizacaoRota(chamados, data) {
    return [...(chamados || [])]
        .filter(chamadoTemLocalizacaoRota)
        .sort((a, b) => {
            const dataA = Date.parse(obterDataReferenciaChamadoRota(a, data)) || 0;
            const dataB = Date.parse(obterDataReferenciaChamadoRota(b, data)) || 0;
            if (dataA !== dataB) return dataB - dataA;
            return obterMinutosHorarioRota(b.horario) - obterMinutosHorarioRota(a.horario);
        });
}

function salvarLocalizacaoDerivadaChamadoRota({ tecnicoId, chamado, localizacao, fonteLocalizacao }) {
    if (!tecnicoId || !chamado || !localizacao || !["chamado_em_execucao", "chamado_finalizado", "ultimo_chamado"].includes(fonteLocalizacao)) return;

    salvarLocalizacaoTecnicoRota({
        tecnicoId,
        chamado,
        coordenada: {
            latitude: localizacao.latitude,
            longitude: localizacao.longitude
        },
        fonteLocalizacao,
        descricao: localizacao.localizacaoDescricao,
        dataReferencia: localizacao.ultimaAtualizacaoLocalizacao
    });
}

function montarLocalizacaoTecnicoPorPrioridadeChamado({ nome, chamadosTecnico, data }) {
    const chamadosOrdenados = ordenarChamadosAtuaisRota(chamadosTecnico);
    const chamadosEmExecucaoComLocalizacao = chamadosOrdenados
        .filter(chamado => isStatusEmExecucaoRota(chamado.status) && !isStatusEncerradoRota(chamado.status))
        .filter(chamadoTemLocalizacaoRota);
    const chamadoEmExecucao = escolherChamadoPrincipalExecucaoRota({
        tecnicoId: nome,
        chamadosExecucao: chamadosEmExecucaoComLocalizacao,
        data
    }).chamado;

    const finalizadoComLocalizacao = ordenarChamadosPorReferenciaLocalizacaoRota(
        chamadosOrdenados.filter(chamado => isStatusEncerradoRota(chamado.status)),
        data
    )[0];

    const primeiroChamadoComLocalizacao = chamadosOrdenados.find(chamadoTemLocalizacaoRota);
    const prioridades = [
        [chamadoEmExecucao, "chamado_em_execucao"],
        [finalizadoComLocalizacao, "chamado_finalizado"],
        [primeiroChamadoComLocalizacao, "primeiro_chamado_do_dia"]
    ];

    for (const [chamado, fonteLocalizacao] of prioridades) {
        if (!chamado) continue;

        const localizacao = montarLocalizacaoTecnicoPorChamado({
            chamado,
            data,
            fonteLocalizacao
        });

        salvarLocalizacaoDerivadaChamadoRota({
            tecnicoId: nome,
            chamado,
            localizacao,
            fonteLocalizacao
        });

        return localizacao;
    }

    return null;
}

function obterCasaTecnicoPontosFixos(pontosFixos, tecnicoId) {
    return (pontosFixos || [])
        .filter(ponto => ponto.tipo === "casa_tecnico")
        .filter(ponto => String(ponto.tecnicoId || ponto.tecnico_id || "") === String(tecnicoId))
        .filter(validarCoordenadaRota)
        .sort((a, b) => String(b.updatedAt || b.updated_at || "").localeCompare(String(a.updatedAt || a.updated_at || "")))[0] || null;
}

function obterPopPadraoPontosFixos(pontosFixos) {
    return (pontosFixos || [])
        .filter(ponto => ponto.tipo === "pop")
        .filter(validarCoordenadaRota)
        .sort((a, b) => String(a.nome || "").localeCompare(String(b.nome || ""), "pt-BR"))[0] || null;
}

function montarLocalizacaoTecnicoPorPontoFixo({ ponto, fonteLocalizacao, descricao }) {
    const coordenada = validarCoordenadaRota(ponto);
    if (!coordenada) return montarLocalizacaoDesconhecidaRota();

    return {
        latitude: coordenada.latitude,
        longitude: coordenada.longitude,
        fonteLocalizacao,
        origemLocalizacao: fonteLocalizacao,
        localizacaoDescricao: descricao,
        ultimaAtualizacaoLocalizacao: ponto.updatedAt || ponto.updated_at || ponto.createdAt || ponto.created_at || null,
        chamadoReferenciaLocalizacaoId: null,
        chamadoReferenciaLocalizacaoCliente: null,
        pontoFixoReferenciaId: ponto.id,
        pontoFixoReferenciaNome: ponto.nome,
        pontoFixoReferenciaTipo: ponto.tipo,
        statusPosicaoGps: "sem_gps",
        empresaRastreame: null,
        vinculoRastreameOrigem: null,
        veiculo: null
    };
}

function montarLocalizacaoOperacionalTecnico({ nome, chamadosTecnico, data, localizacaoSalva, posicaoRastreame, pontosFixos = [] }) {
    if (isTecnicoSemRastreamentoGps(nome)) {
        return montarLocalizacaoTecnicoPorPrioridadeChamado({ nome, chamadosTecnico, data })
            || montarLocalizacaoDesconhecidaRota();
    }

    const coordenadaGps = validarCoordenadaRota(posicaoRastreame);

    if (coordenadaGps) {
        return {
            latitude: coordenadaGps.latitude,
            longitude: coordenadaGps.longitude,
            fonteLocalizacao: "gps_rastreame",
            origemLocalizacao: "gps_rastreame",
            empresaRastreame: posicaoRastreame.fonte,
            localizacaoDescricao: "Localização atual do veículo via RastreaMe",
            ultimaAtualizacaoLocalizacao: posicaoRastreame.dataPosicao || new Date().toISOString(),
            chamadoReferenciaLocalizacaoId: null,
            chamadoReferenciaLocalizacaoCliente: null,
            statusPosicaoGps: posicaoRastreame.statusPosicaoGps || "sem_gps",
            vinculoRastreameOrigem: posicaoRastreame.vinculoOrigem || null,
            veiculo: {
                fonte: posicaoRastreame.fonte,
                placa: posicaoRastreame.placa || "",
                nome: posicaoRastreame.nomeVeiculo || "",
                nomeTecnicoApi: posicaoRastreame.nomeTecnicoRastreame || posicaoRastreame.nomeTecnicoApi || "",
                vinculoOrigem: posicaoRastreame.vinculoOrigem || null,
                confiancaVinculo: posicaoRastreame.confiancaVinculo || null,
                velocidade: posicaoRastreame.velocidade,
                ignicao: posicaoRastreame.ignicao,
                ultimaPosicao: posicaoRastreame.dataPosicao || null,
                endereco: posicaoRastreame.endereco || ""
            }
        };
    }

    if (localizacaoSalva && fonteSalvaTemPrioridadeRota(localizacaoSalva)) {
        return {
            latitude: localizacaoSalva.latitude,
            longitude: localizacaoSalva.longitude,
            fonteLocalizacao: localizacaoSalva.fonte_localizacao || localizacaoSalva.origem || "manual",
            origemLocalizacao: localizacaoSalva.origem || localizacaoSalva.fonte_localizacao || "manual",
            localizacaoDescricao: localizacaoSalva.descricao || "Localização manual do técnico.",
            ultimaAtualizacaoLocalizacao: localizacaoSalva.data_referencia || localizacaoSalva.updated_at || localizacaoSalva.created_at,
            chamadoReferenciaLocalizacaoId: localizacaoSalva.chamado_referencia_id || null,
            chamadoReferenciaLocalizacaoCliente: null,
            statusPosicaoGps: "sem_gps",
            empresaRastreame: null,
            vinculoRastreameOrigem: null,
            veiculo: null
        };
    }

    const localizacaoPorChamado = montarLocalizacaoTecnicoPorPrioridadeChamado({ nome, chamadosTecnico, data });
    if (localizacaoPorChamado) return localizacaoPorChamado;

    if (localizacaoSalva && fonteSalvaManualRota(localizacaoSalva)) {
        return {
            latitude: localizacaoSalva.latitude,
            longitude: localizacaoSalva.longitude,
            fonteLocalizacao: localizacaoSalva.fonte_localizacao || localizacaoSalva.origem || "manual",
            origemLocalizacao: localizacaoSalva.origem || localizacaoSalva.fonte_localizacao || "manual",
            localizacaoDescricao: localizacaoSalva.descricao || "Localização manual do técnico.",
            ultimaAtualizacaoLocalizacao: localizacaoSalva.data_referencia || localizacaoSalva.updated_at || localizacaoSalva.created_at,
            chamadoReferenciaLocalizacaoId: localizacaoSalva.chamado_referencia_id || null,
            chamadoReferenciaLocalizacaoCliente: null,
            statusPosicaoGps: "sem_gps",
            empresaRastreame: null,
            vinculoRastreameOrigem: null,
            veiculo: null
        };
    }

    const casaTecnico = obterCasaTecnicoPontosFixos(pontosFixos, nome);
    if (casaTecnico) {
        return montarLocalizacaoTecnicoPorPontoFixo({
            ponto: casaTecnico,
            fonteLocalizacao: "casa_tecnico",
            descricao: "Localização inicial: casa cadastrada do técnico"
        });
    }

    const popPadrao = obterPopPadraoPontosFixos(pontosFixos);
    if (popPadrao) {
        return montarLocalizacaoTecnicoPorPontoFixo({
            ponto: popPadrao,
            fonteLocalizacao: "pop_base",
            descricao: `Localização inicial: POP / Base ${popPadrao.nome}`
        });
    }

    return montarLocalizacaoDesconhecidaRota();
}

function montarTecnicosCoordenadorRota(chamados, filtros, chamadosBaseLocalizacao = chamados, pontosFixos = []) {
    const localizacoes = listarLocalizacoesTecnicosRota();
    const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreame();
    const rastreamePorTecnico = obterMapaPosicoesTecnicosRastreame({ tecnicos: tecnicosAgendaRastreame });
    const ausencias = new Set(listarAusenciasOperacionaisTecnicosPorData(filtros.data).map(item => item.technician_id));
    const nomesTecnicos = [...obterTecnicosValidosCoordenadorRota(filtros)].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const tecnicoFiltro = normalizarTextoRota(filtros.tecnicoId);
    const regiaoFiltro = normalizarTextoRota(filtros.regiao);

    return nomesTecnicos.map((nome, index) => {
        const chamadosTecnico = chamados.filter(chamado => chamado.tecnicoAtualId === nome);
        const chamadosTecnicoBase = chamadosBaseLocalizacao.filter(chamado => chamado.tecnicoAtualId === nome);
        const regiao = obterRegiaoPrincipalChamados(chamadosTecnico);
        const localizacaoSalva = localizacoes.get(nome);
        const integracaoSgp = montarInfoSgpTecnico(nome, { incluirInstalacaoSemGps: true });
        const localizacaoOperacional = montarLocalizacaoOperacionalTecnico({
            nome,
            chamadosTecnico: chamadosTecnicoBase,
            data: filtros.data,
            localizacaoSalva,
            posicaoRastreame: rastreamePorTecnico.get(nome),
            pontosFixos
        });
        const chamadosPendentes = chamadosTecnico.filter(chamado => !isStatusEncerradoRota(chamado.status));

        return {
            id: nome,
            nome,
            regiao,
            status: ausencias.has(nome) ? "ausente" : (chamadosPendentes.length ? "em_rota" : "disponivel"),
            usuarioSgp: integracaoSgp.usuarioSgp,
            sgpAtivo: integracaoSgp.sgpAtivo,
            sgpVinculado: integracaoSgp.sgpVinculado,
            statusIntegracaoSgp: integracaoSgp.statusIntegracaoSgp,
            statusIntegracaoSgpLabel: integracaoSgp.statusIntegracaoSgpLabel,
            latitude: localizacaoOperacional.latitude,
            longitude: localizacaoOperacional.longitude,
            fonteLocalizacao: localizacaoOperacional.fonteLocalizacao,
            fonteLocalizacaoLabel: obterDescricaoFonteLocalizacaoRota(localizacaoOperacional.fonteLocalizacao),
            localizacaoDescricao: localizacaoOperacional.localizacaoDescricao,
            ultimaAtualizacao: localizacaoOperacional.ultimaAtualizacaoLocalizacao,
            ultimaAtualizacaoLocalizacao: localizacaoOperacional.ultimaAtualizacaoLocalizacao,
            chamadoReferenciaLocalizacaoId: localizacaoOperacional.chamadoReferenciaLocalizacaoId,
            chamadoReferenciaLocalizacaoCliente: localizacaoOperacional.chamadoReferenciaLocalizacaoCliente,
            pontoFixoReferenciaId: localizacaoOperacional.pontoFixoReferenciaId || null,
            pontoFixoReferenciaNome: localizacaoOperacional.pontoFixoReferenciaNome || null,
            pontoFixoReferenciaTipo: localizacaoOperacional.pontoFixoReferenciaTipo || null,
            origemLocalizacao: localizacaoOperacional.origemLocalizacao,
            empresaRastreame: localizacaoOperacional.empresaRastreame,
            vinculoRastreameOrigem: localizacaoOperacional.vinculoRastreameOrigem,
            statusPosicaoGps: localizacaoOperacional.statusPosicaoGps,
            veiculo: localizacaoOperacional.veiculo,
            chamadosAtribuidos: chamadosTecnico.length,
            chamadosPendentes: chamadosPendentes.length,
            distanciaEstimadaKm: 0,
            tempoEstimadoMin: 0,
            cor: getTecnicoColor(nome, index)
        };
    }).filter(tecnico => {
        if (tecnicoFiltro && tecnicoFiltro !== "todos" && normalizarTextoRota(tecnico.id) !== tecnicoFiltro) return false;
        if (regiaoFiltro && regiaoFiltro !== "todos" && tecnico.regiao !== "GERAL" && normalizarTextoRota(tecnico.regiao) !== regiaoFiltro) return false;

        return true;
    });
}

function obterMinutosHorarioRota(horario) {
    const partes = String(horario || "").match(/(\d{1,2}):(\d{2})/);
    if (!partes) return Number.POSITIVE_INFINITY;

    return (Number(partes[1]) * 60) + Number(partes[2]);
}

function ordenarChamadosAtuaisRota(chamados) {
    return [...chamados].sort((a, b) => {
        const difHorario = obterMinutosHorarioRota(a.horario) - obterMinutosHorarioRota(b.horario);
        if (difHorario !== 0) return difHorario;

        return String(a.cliente).localeCompare(String(b.cliente), "pt-BR");
    });
}

function chamadoTemLocalizacaoRota(chamado) {
    return Boolean(validarCoordenadaRota(chamado));
}

function ordenarChamadosParaRota(tecnico, chamados) {
    const chamadosComLocalizacao = chamados.filter(chamadoTemLocalizacaoRota);
    const todosTemHorario = chamadosComLocalizacao.every(chamado => Number.isFinite(obterMinutosHorarioRota(chamado.horario)));

    return todosTemHorario
        ? ordenarChamadosAtuaisRota(chamadosComLocalizacao)
        : ordenarChamadosPorProximidade(tecnico, chamadosComLocalizacao);
}

function ordenarChamadosPorProximidade(tecnico, chamados) {
    const restantes = [...chamados];
    const ordenados = [];
    let pontoAtual = tecnico;

    while (restantes.length) {
        let melhorIndice = 0;
        let melhorDistancia = Number.POSITIVE_INFINITY;

        restantes.forEach((chamado, index) => {
            const distancia = calcularDistanciaKm(pontoAtual, chamado);
            if (distancia < melhorDistancia) {
                melhorIndice = index;
                melhorDistancia = distancia;
            }
        });

        const [proximo] = restantes.splice(melhorIndice, 1);
        ordenados.push(proximo);
        pontoAtual = proximo;
    }

    return ordenados;
}

function obterChamadosDestinoRota(tecnico, chamados) {
    if (!validarCoordenadaRota(tecnico)) return [];

    return chamados
        .filter(chamado => chamado.tecnicoAtualId === tecnico.id)
        .filter(chamadoTemLocalizacaoRota)
        .filter(chamado => !isStatusEncerradoRota(chamado.status))
        .filter(chamado => {
            if (!["chamado_em_execucao", "primeiro_chamado_do_dia", "ultimo_chamado"].includes(tecnico.fonteLocalizacao)) return true;
            return String(chamado.id) !== String(tecnico.chamadoReferenciaLocalizacaoId || "");
        });
}

function calcularDistanciaSequenciaRota(tecnico, chamados) {
    let distanciaTotal = 0;
    let pontoAtual = tecnico;

    chamados.forEach(chamado => {
        distanciaTotal += calcularDistanciaKm(pontoAtual, chamado);
        pontoAtual = chamado;
    });

    return Number(distanciaTotal.toFixed(2));
}

function montarRotasCoordenadorRota(tecnicos, chamados, data) {
    const tecnicosPorId = new Map(tecnicos.map(tecnico => [tecnico.id, tecnico]));

    const rotas = tecnicos.map(tecnico => {
        const tecnicoTemLocalizacao = Boolean(validarCoordenadaRota(tecnico));
        const tecnicoSemUsuarioSgp = tecnico.statusIntegracaoSgp === "usuario_nao_vinculado";
        const chamadosDestino = obterChamadosDestinoRota(tecnico, chamados);
        const chamadosTecnico = ordenarChamadosParaRota(tecnico, chamadosDestino);
        const distanciaTotalKm = calcularDistanciaSequenciaRota(tecnico, chamadosTecnico);
        const tempoTotalMin = estimarTempoRotaMin(distanciaTotalKm, chamadosTecnico.length);
        const proximoChamado = chamadosTecnico[0] || null;
        const distanciaProximoKm = proximoChamado ? calcularDistanciaKm(tecnico, proximoChamado) : 0;
        const tempoProximoMin = proximoChamado ? estimarTempoRotaMin(distanciaProximoKm, 0) : 0;
        let previsaoAcumuladaMin = 0;

        tecnico.distanciaEstimadaKm = distanciaTotalKm;
        tecnico.tempoEstimadoMin = tempoTotalMin;
        tecnico.proximoDestinoRota = proximoChamado ? {
            chamado_id: proximoChamado.id,
            cliente: proximoChamado.cliente,
            bairro: proximoChamado.bairro,
            status: proximoChamado.status,
            prioridade: proximoChamado.prioridade,
            horario: proximoChamado.horario,
            distancia_km: distanciaProximoKm,
            tempo_min: tempoProximoMin,
            previsao_chegada_min: tempoProximoMin
        } : null;

        const itens = chamadosTecnico.map((chamado, index) => {
            chamado.ordemAtendimento = index + 1;
            const distanciaEstimativaKm = index === 0
                ? calcularDistanciaKm(tecnico, chamado)
                : calcularDistanciaKm(chamadosTecnico[index - 1], chamado);
            const tempoEstimadoMin = estimarTempoRotaMin(distanciaEstimativaKm, 0);
            previsaoAcumuladaMin += tempoEstimadoMin;
            const previsaoChegadaMin = previsaoAcumuladaMin;
            previsaoAcumuladaMin += MINUTOS_ATENDIMENTO_ROTA;

            return {
                chamado_id: chamado.id,
                ordem: index + 1,
                latitude: chamado.latitude,
                longitude: chamado.longitude,
                cliente: chamado.cliente,
                status: chamado.status,
                prioridade: chamado.prioridade,
                horario: chamado.horario,
                tempo_estimado_min: tempoEstimadoMin,
                distancia_estimativa_km: distanciaEstimativaKm,
                previsao_chegada_min: previsaoChegadaMin
            };
        });

        return {
            id: `${data}-${tecnico.id}`,
            data_rota: data,
            tecnico_id: tecnico.id,
            tecnico_nome: tecnico.nome,
            cor: tecnico.cor,
            status: tecnicoSemUsuarioSgp ? "usuario_sgp_nao_vinculado" : (!tecnicoTemLocalizacao ? "sem_localizacao" : (chamadosTecnico.length ? "planejada" : "sem_chamados")),
            fonteLocalizacao: tecnico.fonteLocalizacao,
            pontoInicialDescricao: tecnico.localizacaoDescricao,
            chamadoReferenciaLocalizacaoId: tecnico.chamadoReferenciaLocalizacaoId,
            distancia_total_km: distanciaTotalKm,
            tempo_total_min: tempoTotalMin,
            distancia_proximo_km: distanciaProximoKm,
            tempo_proximo_min: tempoProximoMin,
            proximo_chamado: tecnico.proximoDestinoRota,
            itens,
            pontos: tecnicoTemLocalizacao ? [
                {
                    latitude: tecnico.latitude,
                    longitude: tecnico.longitude,
                    tipo: "tecnico",
                    fonteLocalizacao: tecnico.fonteLocalizacao,
                    descricao: tecnico.localizacaoDescricao
                },
                ...chamadosTecnico.map(chamado => ({
                    latitude: chamado.latitude,
                    longitude: chamado.longitude,
                    tipo: "chamado",
                    chamado_id: chamado.id,
                    ordem: chamado.ordemAtendimento
                }))
            ] : []
        };
    });

    chamados.forEach(chamado => {
        if (!tecnicosPorId.has(chamado.tecnicoAtualId)) {
            chamado.ordemAtendimento = 0;
        }
    });

    return rotas;
}

function criarChaveSugestaoRota(sugestao) {
    return [
        sugestao.data_sugestao,
        sugestao.tipo_sugestao,
        sugestao.chamado_id || "rota",
        sugestao.tecnico_atual_id || "",
        sugestao.tecnico_sugerido_id || "",
        (sugestao.ordem_sugerida || []).join("-")
    ].join("|");
}

function gerarSugestoesCoordenadorRota({ data, tecnicos, chamados, rotas }) {
    const sugestoes = [];
    const tecnicosDisponiveis = tecnicos.filter(tecnico => tecnico.status !== "ausente");
    const tecnicosPorId = new Map(tecnicosDisponiveis.map(tecnico => [tecnico.id, tecnico]));

    chamados
        .filter(chamado => !isStatusEncerradoRota(chamado.status))
        .filter(chamadoTemLocalizacaoRota)
        .forEach(chamado => {
            if (!tecnicosDisponiveis.length) return;

            const tecnicoAtual = tecnicosPorId.get(chamado.tecnicoAtualId);
            const candidatos = tecnicosDisponiveis.map(tecnico => ({
                tecnico,
                distanciaKm: calcularDistanciaKm(tecnico, chamado)
            })).sort((a, b) => a.distanciaKm - b.distanciaKm);
            const melhor = candidatos[0];

            if (!melhor) return;

            const distanciaAtualKm = tecnicoAtual
                ? calcularDistanciaKm(tecnicoAtual, chamado)
                : melhor.distanciaKm + 2;
            const economiaKm = Math.max(0, Number((distanciaAtualKm - melhor.distanciaKm).toFixed(2)));
            const economiaMin = Math.max(0, Math.round(economiaKm / VELOCIDADE_MEDIA_ROTA_KMH * 60));
            const atualNome = tecnicoAtual?.nome || chamado.tecnicoAtualNome || "sem técnico";
            const isCritico = chamado.prioridade === "critica";
            const deveSugerirTroca = melhor.tecnico.id !== chamado.tecnicoAtualId && (economiaKm >= 0.7 || (isCritico && economiaKm >= 0.25));

            if (deveSugerirTroca) {
                sugestoes.push({
                    data_sugestao: data,
                    tipo_sugestao: "mover_chamado",
                    chamado_id: chamado.id,
                    cliente: chamado.cliente,
                    regiao: chamado.regiao,
                    status_chamado: chamado.status,
                    motivo_chamado: chamado.motivo,
                    tecnico_atual_id: chamado.tecnicoAtualId || null,
                    tecnico_atual_nome: atualNome,
                    tecnico_sugerido_id: melhor.tecnico.id,
                    tecnico_sugerido_nome: melhor.tecnico.nome,
                    motivo: `Sugestão: mover este chamado do técnico ${atualNome} para ${melhor.tecnico.nome}.`,
                    mensagem: `Técnico ${melhor.tecnico.nome} está mais próximo do chamado da cliente ${chamado.cliente}.`,
                    detalhe: `Economia estimada: ${formatarMinutosRota(economiaMin)} / ${formatarKmRota(economiaKm)}.`,
                    prioridade: chamado.prioridade,
                    economia_tempo_min: economiaMin,
                    economia_km: economiaKm,
                    ordem_sugerida: []
                });
            }

            if (isCritico && distanciaAtualKm >= 8) {
                sugestoes.push({
                    data_sugestao: data,
                    tipo_sugestao: "chamado_critico_distante",
                    chamado_id: chamado.id,
                    cliente: chamado.cliente,
                    regiao: chamado.regiao,
                    status_chamado: chamado.status,
                    motivo_chamado: chamado.motivo,
                    tecnico_atual_id: chamado.tecnicoAtualId || null,
                    tecnico_atual_nome: atualNome,
                    tecnico_sugerido_id: melhor.tecnico.id,
                    tecnico_sugerido_nome: melhor.tecnico.nome,
                    motivo: "Chamado crítico longe do técnico atual.",
                    mensagem: `Chamado crítico da cliente ${chamado.cliente} está longe do técnico atual.`,
                    detalhe: `Técnico mais próximo: ${melhor.tecnico.nome}. Distância atual: ${formatarKmRota(distanciaAtualKm)}.`,
                    prioridade: "critica",
                    economia_tempo_min: economiaMin,
                    economia_km: economiaKm,
                    ordem_sugerida: []
                });
            }
        });

    rotas.forEach(rota => {
        const tecnico = tecnicosPorId.get(rota.tecnico_id);
        if (!tecnico || rota.itens.length < 3) return;

        const chamadosRota = rota.itens
            .map(item => chamados.find(chamado => chamado.id === item.chamado_id))
            .filter(Boolean);
        const ordemAtual = chamadosRota.map(chamado => chamado.id);
        const ordemProxima = ordenarChamadosPorProximidade(tecnico, chamadosRota);
        const ordemSugerida = ordemProxima.map(chamado => chamado.id);

        if (ordemAtual.join("|") === ordemSugerida.join("|")) return;

        const distanciaAtual = calcularDistanciaSequenciaRota(tecnico, chamadosRota);
        const distanciaSugerida = calcularDistanciaSequenciaRota(tecnico, ordemProxima);
        const economiaKm = Number((distanciaAtual - distanciaSugerida).toFixed(2));

        if (economiaKm < 1) return;

        const economiaMin = Math.round(economiaKm / VELOCIDADE_MEDIA_ROTA_KMH * 60);

        sugestoes.push({
            data_sugestao: data,
            tipo_sugestao: "reorganizar_rota",
            chamado_id: null,
            cliente: "",
            regiao: tecnico.regiao,
            status_chamado: "",
            motivo_chamado: "",
            tecnico_atual_id: tecnico.id,
            tecnico_atual_nome: tecnico.nome,
            tecnico_sugerido_id: tecnico.id,
            tecnico_sugerido_nome: tecnico.nome,
            motivo: "Rota atual pode ser reorganizada para reduzir deslocamento.",
            mensagem: `Rota de ${tecnico.nome} pode ser reorganizada pelo ponto mais próximo.`,
            detalhe: `Economia estimada: ${formatarMinutosRota(economiaMin)} / ${formatarKmRota(economiaKm)}.`,
            prioridade: "normal",
            economia_tempo_min: economiaMin,
            economia_km: economiaKm,
            ordem_sugerida: ordemSugerida
        });
    });

    return sugestoes.map(sugestao => ({
        ...sugestao,
        chave: criarChaveSugestaoRota(sugestao)
    }));
}

function filtrarSugestoesCoordenadorRota(sugestoes, filtros) {
    const regiao = normalizarTextoRota(filtros.regiao);
    const tecnicoId = normalizarTextoRota(filtros.tecnicoId);
    const status = normalizarTextoRota(filtros.status);
    const motivo = normalizarTextoRota(filtros.motivo);
    const filtrarEscopoInstalacao = isEscopoInstalacaoCoordenadorRota(filtros);
    const tecnicosEscopo = filtrarEscopoInstalacao
        ? obterTecnicosValidosCoordenadorRota(filtros)
        : null;

    return sugestoes
        .filter(item => item.data_sugestao === filtros.data)
        .filter(item => {
            if (filtrarEscopoInstalacao) {
                const tecnicosRelacionados = [
                    item.tecnico_atual_id,
                    item.tecnico_sugerido_id
                ].filter(Boolean);

                if (!tecnicosRelacionados.length || tecnicosRelacionados.some(tecnico => !tecnicoPermitidoCoordenadorRota(tecnico, tecnicosEscopo))) {
                    return false;
                }
            }
            if (regiao && regiao !== "todos" && normalizarTextoRota(item.regiao) !== regiao) return false;
            if (tecnicoId && tecnicoId !== "todos") {
                const tecnicoAtual = normalizarTextoRota(item.tecnico_atual_id);
                const tecnicoSugerido = normalizarTextoRota(item.tecnico_sugerido_id);
                if (tecnicoAtual !== tecnicoId && tecnicoSugerido !== tecnicoId) return false;
            }
            if (status && status !== "todos" && normalizarTextoRota(item.status_chamado) !== status) return false;
            if (motivo && motivo !== "todos" && !normalizarTextoRota(`${item.motivo_chamado} ${item.motivo} ${item.mensagem} ${item.detalhe}`).includes(motivo)) return false;

            return true;
        })
        .sort((a, b) => {
            const prioridade = Number(a.prioridade === "critica") - Number(b.prioridade === "critica");
            if (prioridade !== 0) return -prioridade;
            if (a.status !== b.status) return a.status.localeCompare(b.status, "pt-BR");
            return b.economia_tempo_min - a.economia_tempo_min;
        });
}

function salvarSugestoesGeradasRota(sugestoesGeradas, req) {
    const store = lerStoreSugestoesRota();
    const existentes = new Map(store.items.map(item => [item.chave || criarChaveSugestaoRota(item), item]));
    const agora = new Date().toISOString();
    let novas = 0;

    sugestoesGeradas.forEach(sugestao => {
        const existente = existentes.get(sugestao.chave);

        if (existente) {
            if (existente.status === "pendente") {
                existente.mensagem = sugestao.mensagem;
                existente.motivo = sugestao.motivo;
                existente.detalhe = sugestao.detalhe;
                existente.economia_tempo_min = sugestao.economia_tempo_min;
                existente.economia_km = sugestao.economia_km;
                existente.ordem_sugerida = sugestao.ordem_sugerida;
                existente.updated_at = agora;
            }
            return;
        }

        store.items.push({
            id: store.nextId,
            ...sugestao,
            status: "pendente",
            created_at: agora,
            updated_at: agora,
            applied_at: null,
            rejected_at: null,
            usuario_acao: null
        });
        store.nextId += 1;
        novas += 1;
    });

    const normalizado = salvarStoreSugestoesRota(store);
    registrarLogCoordenadorRota("gerar_sugestoes", {
        geradas: sugestoesGeradas.length,
        novas
    }, req);

    return normalizado.items;
}

async function montarDadosCoordenadorRota(filtrosOrigem, req = null) {
    const filtros = normalizarFiltrosCoordenadorRota(filtrosOrigem);
    const opcoesAgenda = { incluirInstalacaoSemGps: true };
    const tecnicosValidosCoordenador = obterTecnicosValidosCoordenadorRota(filtros);
    const pontosFixosBrutos = await listarPontosFixosMapa(obterPerfilPontosFixos(req), tecnicosValidosCoordenador);
    const pontosFixos = filtrarPontosFixosPorEscopoCoordenadorRota(pontosFixosBrutos, filtros);
    const desenhosBrutos = await listarDesenhosMapa({
        dataReferencia: filtros.data,
        perfil: obterPerfilPontosFixos(req),
        tecnicosValidos: tecnicosValidosCoordenador
    });
    const desenhos = filtrarDesenhosPorEscopoCoordenadorRota(desenhosBrutos, filtros);
    const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreameCoordenadorRota(filtros);
    const tecnicosAgendaRastreameServico = obterListaTecnicosAgendaRastreame();
    iniciarSincronizacaoRastreameSeNecessario({ tecnicos: tecnicosAgendaRastreameServico });
    const resultado = await buscarAgenda(filtros.data, opcoesAgenda);
    const agendaEnriquecida = await enriquecerAgendaComContratos(resultado.ordensAgenda, resultado.contratosPendentes, {
        ...opcoesAgenda,
        dataFiltro: filtros.data
    });
    const agenda = agendaEnriquecida || resultado.agenda;
    const chamadosTodos = montarChamadosCoordenadorRota(agenda, filtros.data);
    const chamadosTodosEscopo = filtrarChamadosPorEscopoCoordenadorRota(chamadosTodos, filtros);
    const chamadosSimultaneos = montarResumoChamadosSimultaneosRota(chamadosTodosEscopo, filtros.data);
    const chamados = filtrarChamadosCoordenadorRota(chamadosTodosEscopo, filtros);
    const tecnicos = montarTecnicosCoordenadorRota(chamados, filtros, chamadosTodosEscopo, pontosFixos);
    const chamadosSimultaneosPorTecnico = new Map(chamadosSimultaneos.tecnicos.map(item => [String(item.tecnico_id), item]));
    tecnicos.forEach(tecnico => {
        const infoSimultaneo = chamadosSimultaneosPorTecnico.get(String(tecnico.id));
        tecnico.alertaChamadosSimultaneos = Boolean(infoSimultaneo);
        tecnico.chamadosExecucaoSimultaneos = infoSimultaneo?.total || 0;
        tecnico.chamadosSimultaneos = infoSimultaneo || null;
        tecnico.chamadoPrincipalSimultaneoId = infoSimultaneo?.chamadoPrincipalId || null;
        tecnico.chamadoPrincipalSimultaneoInicio = infoSimultaneo?.chamadoPrincipalInicio || null;
        tecnico.chamadoPrincipalSimultaneoManual = Boolean(infoSimultaneo?.principalManual);
    });
    const rotas = montarRotasCoordenadorRota(tecnicos, chamados, filtros.data);
    const sugestoes = filtrarSugestoesCoordenadorRota(lerStoreSugestoesRota().items, filtros);
    const pendenciasLocalizacao = chamados.filter(chamado => !chamadoTemLocalizacaoRota(chamado));
    const chamadosLocalizacaoSuspeita = chamados.filter(chamado => ["suspeita", "nao_confirmada", "erro_geocoding"].includes(chamado.status_localizacao));
    const chamadosComLocalizacao = chamados.filter(chamadoTemLocalizacaoRota);
    const tecnicosSemUsuarioSgp = tecnicos.filter(tecnico => tecnico.statusIntegracaoSgp === "usuario_nao_vinculado");
    const tecnicosSemLocalizacao = tecnicos.filter(tecnico => !validarCoordenadaRota(tecnico) && tecnico.statusIntegracaoSgp !== "usuario_nao_vinculado");
    const rastreame = filtrarRastreamePorEscopoCoordenadorRota(
        obterVeiculosRastreame({ tecnicos: tecnicosAgendaRastreameServico }),
        filtros
    );
    const gpsTecnicosResumo = {
        atual: tecnicos.filter(tecnico => tecnico.fonteLocalizacao === "gps_rastreame" && tecnico.statusPosicaoGps === "atual").length,
        antigo: tecnicos.filter(tecnico => tecnico.fonteLocalizacao === "gps_rastreame" && tecnico.statusPosicaoGps === "antiga").length,
        desatualizado: tecnicos.filter(tecnico => tecnico.fonteLocalizacao === "gps_rastreame" && tecnico.statusPosicaoGps === "desatualizada").length,
        semVeiculoVinculado: tecnicos.filter(tecnico => tecnico.fonteLocalizacao !== "gps_rastreame").length,
        semLocalizacao: tecnicosSemLocalizacao.length,
        sgpNaoVinculado: tecnicosSemUsuarioSgp.length
    };
    const tecnicoColorMap = Object.fromEntries(tecnicos.map(tecnico => [tecnico.id, tecnico.cor]));
    const regioes = [...new Set(chamadosTodosEscopo.map(chamado => chamado.regiao))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const statusChamados = [...new Set(chamadosTodosEscopo.map(chamado => chamado.status).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));
    const motivos = [...new Set(chamadosTodosEscopo.map(chamado => chamado.motivo).filter(Boolean))].sort((a, b) => a.localeCompare(b, "pt-BR"));

    return {
        filtros,
        permissoes: {
            allowed: true,
            perfisAutorizados: ["admin", "coordenador", "cargo autorizado"],
            modo: "sem autenticacao local",
            pontosFixos: obterPermissoesPontosFixos(req),
            desenhos: obterPermissoesDesenhos(req)
        },
        pontosFixos,
        desenhos,
        tecnicos,
        localizacoesTecnicos: tecnicos.map(tecnico => ({
            tecnico_id: tecnico.id,
            latitude: tecnico.latitude,
            longitude: tecnico.longitude,
            ultimaAtualizacao: tecnico.ultimaAtualizacao,
            origem: tecnico.origemLocalizacao,
            fonteLocalizacao: tecnico.fonteLocalizacao,
            localizacaoDescricao: tecnico.localizacaoDescricao,
            ultimaAtualizacaoLocalizacao: tecnico.ultimaAtualizacaoLocalizacao,
            chamadoReferenciaLocalizacaoId: tecnico.chamadoReferenciaLocalizacaoId,
            chamadoReferenciaLocalizacaoCliente: tecnico.chamadoReferenciaLocalizacaoCliente,
            pontoFixoReferenciaId: tecnico.pontoFixoReferenciaId,
            pontoFixoReferenciaNome: tecnico.pontoFixoReferenciaNome,
            pontoFixoReferenciaTipo: tecnico.pontoFixoReferenciaTipo,
            statusPosicaoGps: tecnico.statusPosicaoGps,
            empresaRastreame: tecnico.empresaRastreame,
            vinculoRastreameOrigem: tecnico.vinculoRastreameOrigem,
            veiculo: tecnico.veiculo,
            usuarioSgp: tecnico.usuarioSgp,
            sgpAtivo: tecnico.sgpAtivo,
            sgpVinculado: tecnico.sgpVinculado,
            statusIntegracaoSgp: tecnico.statusIntegracaoSgp,
            statusIntegracaoSgpLabel: tecnico.statusIntegracaoSgpLabel
        })),
        chamados,
        rotas,
        sugestoes,
        chamadosSimultaneos,
        rastreame,
        gpsTecnicosResumo,
        tecnicosSemLocalizacao,
        tecnicosSemUsuarioSgp,
        pendenciasLocalizacao,
        chamadosSemLocalizacao: pendenciasLocalizacao,
        chamadosLocalizacaoSuspeita,
        tecnicoColorMap,
        localizacaoResumo: {
            totalChamados: chamados.length,
            comLocalizacao: chamadosComLocalizacao.length,
            semLocalizacao: pendenciasLocalizacao.length,
            suspeitos: chamadosLocalizacaoSuspeita.length
        },
        resumo: {
            tecnicos: tecnicos.length,
            chamados: chamados.length,
            chamadosComLocalizacao: chamadosComLocalizacao.length,
            chamadosSemLocalizacao: pendenciasLocalizacao.length,
            chamadosLocalizacaoSuspeita: chamadosLocalizacaoSuspeita.length,
            chamadosCriticos: chamados.filter(chamado => chamado.prioridade === "critica").length,
            tecnicosComChamadosSimultaneos: chamadosSimultaneos.totalTecnicos,
            sugestoesPendentes: sugestoes.filter(sugestao => sugestao.status === "pendente").length,
            distanciaTotalKm: Number(rotas.reduce((total, rota) => total + rota.distancia_total_km, 0).toFixed(2)),
            tempoTotalMin: rotas.reduce((total, rota) => total + rota.tempo_total_min, 0)
        },
        opcoes: {
            regioes,
            tecnicos: [...tecnicosValidosCoordenador].sort((a, b) => a.localeCompare(b, "pt-BR")),
            tecnicosRastreame: [...tecnicosAgendaRastreame].sort((a, b) => a.localeCompare(b, "pt-BR")),
            status: statusChamados,
            motivos,
            pontosFixosTipos: obterTiposPontosFixos(),
            pontosFixosVisibilidades: obterVisibilidadesPontosFixos(),
            desenhosFerramentas: obterFerramentasDesenhoMapa(),
            desenhosTiposArea: obterTiposAreaDesenhoMapa(),
            desenhosVisibilidades: obterVisibilidadesDesenhoMapa()
        }
    };
}

function limparCachesSgpCoordenadorRota({ data, contratoIds = [] } = {}) {
    if (data) {
        agendaCache.delete(data);
        agendaRequests.delete(data);
        agendaEnriquecidaCache.clear();
        agendaEnriquecidaRequests.clear();
        STATUS_ORDEM_SERVICO.forEach(status => {
            const cacheKey = getOrdemServicoStatusCacheKey(status, data);
            ordemServicoStatusCache.delete(cacheKey);
            ordemServicoStatusRequests.delete(cacheKey);
        });
    }

    const contratosNormalizados = contratoIds.filter(Boolean).map(String);

    if (contratosNormalizados.length) {
        agendaEnriquecidaCache.clear();
        agendaEnriquecidaRequests.clear();
    }

    contratosNormalizados.forEach(contratoId => {
        contratoCache.delete(contratoId);
        contratoRequests.delete(contratoId);
    });
}

function obterRegistroLocalizacaoChamado(store, chamadoId, enderecoCompleto) {
    const chave = montarChaveLocalizacaoChamado(chamadoId, enderecoCompleto);
    const porChave = store.items.find(item => item.chave === chave);
    if (porChave) return porChave;

    return store.items
        .filter(item => String(item.chamado_id) === String(chamadoId))
        .sort((a, b) => Date.parse(b.updated_at || b.created_at || 0) - Date.parse(a.updated_at || a.created_at || 0))[0] || null;
}

function sincronizarItemLocalizacaoSgp({
    os,
    index,
    data,
    agora,
    chamadoIdAlvo = null,
    substituirManual = false,
    localizacoesStore = null
}) {
    const chamadoId = obterChamadoIdCoordenadorRota(os, data, index);

    if (chamadoIdAlvo && String(chamadoId) !== String(chamadoIdAlvo)) {
        return null;
    }

    const enderecoSgp = normalizarEnderecoSgp(os);
    const enderecoCompleto = enderecoSgp.enderecoCompleto || obterEnderecoCompletoChamado(os);
    const coordenadaSgp = validarCoordenadaRota(enderecoSgp) || parseCoordenadasRota(os.localizacao);
    const store = localizacoesStore || lerStoreLocalizacoesChamados();
    const salvarOpcoes = {
        store: localizacoesStore || undefined,
        salvar: !localizacoesStore
    };
    const cached = obterRegistroLocalizacaoChamado(store, chamadoId, enderecoCompleto);
    const coordenadaAtual = validarCoordenadaRota(cached);
    const protegidaManual = Boolean(cached && coordenadaAtual && isLocalizacaoManualProtegidaRota(cached));
    const diferente = coordenadasDiferentesRota(coordenadaAtual, coordenadaSgp);
    const statusAnterior = normalizarStatusLocalizacaoRota(cached?.status_localizacao);
    const corrigiuSemLocalizacao = Boolean(coordenadaSgp && !protegidaManual && (
        !coordenadaAtual
        || ["sem_localizacao", "erro_geocoding", "nao_confirmada", "suspeita"].includes(statusAnterior)
    ));

    if (!coordenadaSgp) {
        salvarLocalizacaoChamadoCache({
            chamadoId,
            enderecoCompleto,
            coordenada: coordenadaAtual,
            statusLocalizacao: cached?.status_localizacao || "sem_localizacao",
            origem: cached?.origem || "sgp_sincronizado",
            fonteLocalizacao: cached?.fonte_localizacao || cached?.origem || "desconhecida",
            sgpCoordenada: null,
            sgpAtualizadoEm: agora,
            ultimaSincronizacaoSgp: agora,
            enderecoSgp,
            preservarCoordenadaAtual: Boolean(coordenadaAtual),
            observacao: enderecoCompleto
                ? "SGP retornou endereço, mas não retornou latitude/longitude."
                : "SGP não retornou endereço nem latitude/longitude na sincronização."
        }, salvarOpcoes);

        return {
            chamadoId,
            status: "sem_coordenada_sgp",
            coordenadaSgp: null,
            coordenadaAnterior: coordenadaAtual,
            conflitoManual: false,
            corrigiuSemLocalizacao: false,
            enderecoSgp
        };
    }

    if (protegidaManual && diferente && !substituirManual) {
        salvarLocalizacaoChamadoCache({
            chamadoId,
            enderecoCompleto,
            coordenada: coordenadaAtual,
            statusLocalizacao: cached.status_localizacao,
            origem: cached.origem || "manual",
            fonteLocalizacao: cached.fonte_localizacao || cached.origem || "manual",
            sgpCoordenada: coordenadaSgp,
            sgpAtualizadoEm: agora,
            ultimaSincronizacaoSgp: agora,
            enderecoSgp,
            preservarCoordenadaAtual: true,
            observacao: "SGP possui coordenada diferente da localização manual."
        }, salvarOpcoes);

        return {
            chamadoId,
            status: "conflito_manual",
            coordenadaSgp,
            coordenadaAnterior: coordenadaAtual,
            conflitoManual: true,
            corrigiuSemLocalizacao: false,
            enderecoSgp
        };
    }

    const atualizou = diferente || !coordenadaAtual || !isFonteSgpLocalizacaoRota(cached) || substituirManual;

    salvarLocalizacaoChamadoCache({
        chamadoId,
        enderecoCompleto,
        coordenada: coordenadaSgp,
        statusLocalizacao: "confirmada",
        origem: "sgp_sincronizado",
        fonteLocalizacao: "sgp",
        sgpCoordenada: coordenadaSgp,
        sgpAtualizadoEm: agora,
        ultimaSincronizacaoSgp: agora,
        enderecoSgp,
        observacao: atualizou ? "Coordenada atualizada a partir do SGP." : "Coordenada SGP confirmada sem alteração."
    }, salvarOpcoes);

    return {
        chamadoId,
        status: atualizou ? "atualizado" : "sem_alteracao",
        coordenadaSgp,
        coordenadaAnterior: coordenadaAtual,
        conflitoManual: false,
        corrigiuSemLocalizacao,
        enderecoSgp
    };
}

async function sincronizarLocalizacoesSgpCoordenadorRota(filtrosOrigem, req = null, chamadoIdAlvo = null, opcoes = {}) {
    const filtros = normalizarFiltrosCoordenadorRota(filtrosOrigem);
    const agora = new Date().toISOString();
    const substituirManual = Boolean(opcoes.substituirManual);
    const resumo = {
        sucesso: true,
        data: filtros.data,
        chamadoId: chamadoIdAlvo ? String(chamadoIdAlvo) : null,
        totalVerificados: 0,
        atualizados: 0,
        corrigidosDeSemLocalizacao: 0,
        semAlteracao: 0,
        semCoordenadaNoSgp: 0,
        semDadosNoSgp: 0,
        conflitosManuais: 0,
        erros: [],
        conflitos: [],
        atualizadoEm: agora,
        ultimaSincronizacaoSgp: agora
    };

    limparCachesSgpCoordenadorRota({ data: filtros.data });

    const resultadoSgp = await buscarAgendaSgpAtualizada(filtros.data, { incluirInstalacaoSemGps: true });
    const agenda = resultadoSgp.agenda || [];
    const localizacoesStore = lerStoreLocalizacoesChamados();
    let localizacoesAlteradas = false;

    agenda.forEach((os, index) => {
        try {
            const resultado = sincronizarItemLocalizacaoSgp({
                os,
                index,
                data: filtros.data,
                agora,
                chamadoIdAlvo,
                substituirManual,
                localizacoesStore
            });

            if (!resultado) return;
            localizacoesAlteradas = true;

            resumo.totalVerificados += 1;

            if (resultado.status === "atualizado") resumo.atualizados += 1;
            if (resultado.corrigiuSemLocalizacao) resumo.corrigidosDeSemLocalizacao += 1;
            if (resultado.status === "sem_alteracao") resumo.semAlteracao += 1;
            if (resultado.status === "sem_coordenada_sgp") {
                resumo.semCoordenadaNoSgp += 1;
                resumo.semDadosNoSgp += 1;
            }
            if (resultado.status === "conflito_manual") {
                resumo.conflitosManuais += 1;
                resumo.conflitos.push({
                    chamadoId: resultado.chamadoId,
                    coordenadaAtual: resultado.coordenadaAnterior,
                    coordenadaSgp: resultado.coordenadaSgp,
                    mensagem: "O SGP possui uma coordenada diferente da corrigida manualmente."
                });
            }
        } catch (err) {
            resumo.erros.push({
                chamadoId: obterChamadoIdCoordenadorRota(os, filtros.data, index),
                erro: err.message
            });
        }
    });

    if (localizacoesAlteradas) {
        salvarStoreLocalizacoesChamados(localizacoesStore);
    }

    if (chamadoIdAlvo && resumo.totalVerificados === 0) {
        const erro = new Error("Chamado não encontrado na agenda SGP da data informada.");
        erro.statusCode = 404;
        throw erro;
    }

    registrarLogCoordenadorRota(chamadoIdAlvo ? "sincronizar_localizacao_sgp_chamado" : "sincronizar_localizacoes_sgp", {
        data: filtros.data,
        chamadoId: chamadoIdAlvo ? String(chamadoIdAlvo) : null,
        totalVerificados: resumo.totalVerificados,
        atualizados: resumo.atualizados,
        corrigidosDeSemLocalizacao: resumo.corrigidosDeSemLocalizacao,
        semCoordenadaNoSgp: resumo.semCoordenadaNoSgp,
        conflitosManuais: resumo.conflitosManuais
    }, req);

    const dados = await montarDadosCoordenadorRota(filtros, req);
    const chamadoAtualizado = chamadoIdAlvo
        ? (dados.chamados || []).find(chamado => String(chamado.id) === String(chamadoIdAlvo)) || null
        : null;

    dados.sincronizacaoSgp = resumo;

    return {
        ...resumo,
        dados,
        chamadoAtualizado
    };
}

function normalizarCriteriosChamadoSgp(origem = {}) {
    return {
        busca: String(origem.busca || origem.cliente || origem.nome || "").trim(),
        chamadoId: String(origem.chamadoId || origem.chamado_id || origem.id || "").trim(),
        clienteId: String(origem.clienteId || origem.cliente_id || "").trim(),
        contratoId: String(origem.contratoId || origem.contrato_id || origem.contrato || "").trim()
    };
}

function montarTextoBuscaChamadoSgp(item = {}) {
    return normalizarTextoRota([
        item.id,
        item.id_os,
        item.id_ordem_servico,
        item.idOrdemServico,
        item.ordem_servico,
        item.idCliente,
        item.contrato,
        item.id_contrato,
        item.contrato_id,
        item.cliente,
        item.nome,
        item.razao_social,
        item.tecnico,
        item.responsavel,
        item.endereco,
        item.enderecoCompleto,
        item.bairro,
        item.cep
    ].filter(Boolean).join(" "));
}

function chamadoSgpCorrespondeCriterios(item = {}, criterios = {}) {
    const chamadoId = String(item.id || item.id_os || item.id_ordem_servico || item.idOrdemServico || item.ordem_servico || "");
    const contratoId = String(getContratoId(item) || item.idCliente || "");
    const texto = montarTextoBuscaChamadoSgp(item);

    if (criterios.chamadoId && chamadoId === criterios.chamadoId) return true;
    if (criterios.clienteId && String(item.idCliente || item.id_cliente || item.cliente_id || "") === criterios.clienteId) return true;
    if (criterios.contratoId && contratoId === criterios.contratoId) return true;
    if (criterios.busca && texto.includes(normalizarTextoRota(criterios.busca))) return true;

    return false;
}

async function encontrarChamadoSgpCoordenadorRota(filtrosOrigem, criteriosOrigem) {
    const filtros = normalizarFiltrosCoordenadorRota(filtrosOrigem);
    const criterios = normalizarCriteriosChamadoSgp(criteriosOrigem);
    const resultadoSgp = await buscarAgendaSgpAtualizada(filtros.data, { incluirInstalacaoSemGps: true });
    const agenda = resultadoSgp.agenda || [];
    const ordensAgenda = resultadoSgp.ordensAgenda || [];
    let index = agenda.findIndex(item => chamadoSgpCorrespondeCriterios(item, criterios));

    if (index < 0) {
        index = ordensAgenda.findIndex(item => chamadoSgpCorrespondeCriterios(item, criterios));
    }

    const chamadoAgenda = index >= 0 ? agenda[index] : null;
    const ordemBruta = index >= 0 ? ordensAgenda[index] : null;
    const contratoId = criterios.contratoId || getContratoId(ordemBruta) || getContratoId(chamadoAgenda) || chamadoAgenda?.idCliente || "";
    let contratoBruto = null;
    let erroContrato = "";

    if (contratoId) {
        try {
            contratoBruto = await buscarDadosContratoBruto(contratoId);
        } catch (err) {
            erroContrato = err.message;
        }
    }

    return {
        filtros,
        criterios,
        resultadoSgp,
        index,
        chamadoAgenda,
        ordemBruta,
        contratoId,
        contratoBruto,
        erroContrato
    };
}

function sanitizarObjetoDebugCoordenadorRota(valor, depth = 0) {
    if (valor === null || valor === undefined) return valor;
    if (depth > 5) return "[limite]";
    if (Array.isArray(valor)) return valor.slice(0, 10).map(item => sanitizarObjetoDebugCoordenadorRota(item, depth + 1));
    if (typeof valor !== "object") return valor;

    return Object.fromEntries(Object.entries(valor)
        .filter(([chave]) => !normalizarTextoRota(chave).includes("senha") && !normalizarTextoRota(chave).includes("password") && !normalizarTextoRota(chave).includes("token"))
        .slice(0, 80)
        .map(([chave, item]) => [chave, sanitizarObjetoDebugCoordenadorRota(item, depth + 1)]));
}

function obterMotivoSemLocalizacaoChamado({ chamadoFrontend, normalizado, erroContrato, contratoId }) {
    if (validarCoordenadaRota(chamadoFrontend)) return "";
    if (validarCoordenadaRota(normalizado)) return "Campos do SGP mapeados, mas o frontend/cache ainda estava sem coordenada";
    if (erroContrato) return "Erro ao consultar SGP";
    if (!contratoId) return "Cliente sem contrato/endereço vinculado";
    if (normalizado?.enderecoCompleto) return "SGP não retornou latitude/longitude";
    if (!normalizado?.enderecoCompleto) return "SGP não retornou endereço";
    return "Campos do SGP não mapeados";
}

async function montarDebugChamadoCoordenadorRota(filtrosOrigem, criteriosOrigem) {
    const busca = criteriosOrigem.busca || criteriosOrigem.chamadoId || criteriosOrigem.contratoId || "";
    const encontrado = await encontrarChamadoSgpCoordenadorRota(filtrosOrigem, criteriosOrigem);
    const dadosFrontend = await montarDadosCoordenadorRota(encontrado.filtros);
    const chamadoFrontend = (dadosFrontend.chamados || []).find(chamado => {
        if (encontrado.chamadoAgenda && String(chamado.id) === String(encontrado.chamadoAgenda.id)) return true;
        return chamadoSgpCorrespondeCriterios(chamado, encontrado.criterios);
    }) || null;
    const dadosSgpCombinado = {
        ...(encontrado.ordemBruta || {}),
        ...(encontrado.chamadoAgenda || {}),
        ...(encontrado.contratoBruto?.contrato || {})
    };
    const normalizado = normalizarEnderecoSgp(dadosSgpCombinado);

    return {
        busca,
        criterios: encontrado.criterios,
        data: encontrado.filtros.data,
        dadosAgendaLocal: sanitizarObjetoDebugCoordenadorRota(encontrado.chamadoAgenda),
        dadosSgpBruto: sanitizarObjetoDebugCoordenadorRota({
            ordemServico: encontrado.ordemBruta,
            contratoId: encontrado.contratoId,
            contrato: encontrado.contratoBruto?.contrato || null,
            erroContrato: encontrado.erroContrato || null
        }),
        dadosSgpNormalizado: normalizado,
        dadosEnviadosAoFrontend: chamadoFrontend,
        camposEnderecoDetectados: normalizado.camposEnderecoDetectados,
        camposCoordenadaDetectados: normalizado.camposCoordenadaDetectados,
        motivoSemLocalizacao: obterMotivoSemLocalizacaoChamado({
            chamadoFrontend,
            normalizado,
            erroContrato: encontrado.erroContrato,
            contratoId: encontrado.contratoId
        })
    };
}

async function sincronizarLocalizacaoClienteSgpCoordenadorRota(filtrosOrigem, criteriosOrigem, req = null) {
    const encontrado = await encontrarChamadoSgpCoordenadorRota(filtrosOrigem, criteriosOrigem);

    if (!encontrado.chamadoAgenda) {
        const erro = new Error("Chamado/cliente não encontrado na agenda SGP da data informada.");
        erro.statusCode = 404;
        throw erro;
    }

    const agora = new Date().toISOString();
    const localizacoesStore = lerStoreLocalizacoesChamados();
    const resultado = sincronizarItemLocalizacaoSgp({
        os: encontrado.chamadoAgenda,
        index: encontrado.index,
        data: encontrado.filtros.data,
        agora,
        substituirManual: criteriosOrigem.substituirManual === true || criteriosOrigem.substituirManual === "true",
        localizacoesStore
    });
    salvarStoreLocalizacoesChamados(localizacoesStore);
    const resumo = {
        sucesso: true,
        data: encontrado.filtros.data,
        busca: encontrado.criterios.busca || null,
        chamadoId: resultado.chamadoId,
        contratoId: encontrado.contratoId || null,
        totalVerificados: 1,
        atualizados: resultado.status === "atualizado" ? 1 : 0,
        corrigidosDeSemLocalizacao: resultado.corrigiuSemLocalizacao ? 1 : 0,
        semAlteracao: resultado.status === "sem_alteracao" ? 1 : 0,
        semCoordenadaNoSgp: resultado.status === "sem_coordenada_sgp" ? 1 : 0,
        semDadosNoSgp: resultado.status === "sem_coordenada_sgp" ? 1 : 0,
        conflitosManuais: resultado.status === "conflito_manual" ? 1 : 0,
        erros: [],
        conflitos: resultado.status === "conflito_manual" ? [{
            chamadoId: resultado.chamadoId,
            coordenadaAtual: resultado.coordenadaAnterior,
            coordenadaSgp: resultado.coordenadaSgp,
            mensagem: "O SGP possui uma coordenada diferente da corrigida manualmente."
        }] : [],
        atualizadoEm: agora,
        ultimaSincronizacaoSgp: agora
    };

    registrarLogCoordenadorRota("sincronizar_localizacao_sgp_cliente", {
        data: encontrado.filtros.data,
        chamadoId: resultado.chamadoId,
        contratoId: encontrado.contratoId,
        status: resultado.status
    }, req);

    const dados = await montarDadosCoordenadorRota(encontrado.filtros, req);
    const chamadoAtualizado = (dados.chamados || []).find(chamado => String(chamado.id) === String(resultado.chamadoId)) || null;
    dados.sincronizacaoSgp = resumo;

    return {
        ...resumo,
        dados,
        chamadoAtualizado,
        enderecoSgp: resultado.enderecoSgp || null
    };
}

async function reprocessarLocalizacoesCoordenadorRota(filtrosOrigem, req = null) {
    const filtros = normalizarFiltrosCoordenadorRota(filtrosOrigem);
    const resultado = await buscarAgenda(filtros.data, { incluirInstalacaoSemGps: true });
    const agendaEnriquecida = await enriquecerAgendaComContratos(resultado.ordensAgenda, resultado.contratosPendentes, {
        incluirInstalacaoSemGps: true,
        dataFiltro: filtros.data
    });
    const agenda = agendaEnriquecida || resultado.agenda;
    let confirmadas = 0;
    let semLocalizacao = 0;
    const localizacoesStore = lerStoreLocalizacoesChamados();
    let localizacoesAlteradas = false;

    agenda.forEach((os, index) => {
        const chamadoId = String(os.id || os.idOrdemServico || os.idCliente || `${filtros.data}-${index + 1}`);
        const enderecoSgp = normalizarEnderecoSgp(os);
        const enderecoCompleto = enderecoSgp.enderecoCompleto || obterEnderecoCompletoChamado(os);
        const coordenadaApi = validarCoordenadaRota(enderecoSgp) || parseCoordenadasRota(os.localizacao);

        if (coordenadaApi) {
            salvarLocalizacaoChamadoCache({
                chamadoId,
                enderecoCompleto,
                coordenada: coordenadaApi,
                statusLocalizacao: "confirmada",
                origem: "sgp_reprocessado",
                fonteLocalizacao: "sgp",
                sgpCoordenada: coordenadaApi,
                sgpAtualizadoEm: new Date().toISOString(),
                ultimaSincronizacaoSgp: new Date().toISOString(),
                enderecoSgp,
                observacao: "Reprocessado manualmente pelo Coordenador de Rota."
            }, { store: localizacoesStore, salvar: false });
            localizacoesAlteradas = true;
            confirmadas += 1;
            return;
        }

        salvarLocalizacaoChamadoCache({
            chamadoId,
            enderecoCompleto,
            coordenada: null,
            statusLocalizacao: "sem_localizacao",
            origem: "reprocessamento_manual",
            enderecoSgp,
            observacao: enderecoCompleto
                ? "SGP retornou endereço, mas não retornou latitude/longitude no reprocessamento."
                : "Sem endereço e latitude/longitude retornados pela API no reprocessamento."
        }, { store: localizacoesStore, salvar: false });
        localizacoesAlteradas = true;
        semLocalizacao += 1;
    });

    if (localizacoesAlteradas) {
        salvarStoreLocalizacoesChamados(localizacoesStore);
    }

    registrarLogCoordenadorRota("reprocessar_localizacao", {
        data: filtros.data,
        confirmadas,
        semLocalizacao
    }, req);

    const dados = await montarDadosCoordenadorRota(filtros, req);

    return {
        ...dados,
        reprocessamento: {
            confirmadas,
            semLocalizacao
        }
    };
}

function validarPermissaoAdminCoordenadorRota(req, res) {
    const perfil = normalizarTextoRota(req.headers?.["x-user-role"] || req.headers?.["x-user-profile"] || req.headers?.["x-perfil"]);

    if (!perfil) return true;
    if (["admin", "administrador", "coordenador"].some(permitido => perfil.includes(permitido))) return true;

    res.status(403).json({ erro: "Acesso restrito a admin/coordenador." });
    return false;
}

function temContextoAutenticacaoPontosFixos(req) {
    return Boolean(
        req.headers?.["x-user-role"]
        || req.headers?.["x-user-profile"]
        || req.headers?.["x-perfil"]
        || req.headers?.["x-user-permissions"]
        || req.headers?.["x-permissions"]
        || req.headers?.["x-permissoes"]
        || req.headers?.["x-user-permission"]
        || req.headers?.["x-user-id"]
        || req.headers?.["x-user-name"]
        || req.headers?.["x-usuario"]
    );
}

function obterPerfilPontosFixos(req) {
    const perfil = normalizarTextoRota(req?.headers?.["x-user-role"] || req?.headers?.["x-user-profile"] || req?.headers?.["x-perfil"]);
    const semAutenticacaoLocal = !temContextoAutenticacaoPontosFixos(req || { headers: {} });
    const admin = semAutenticacaoLocal || ["admin", "administrador"].some(permitido => perfil.includes(permitido));
    const coordenador = admin || perfil.includes("coordenador");

    return {
        admin,
        coordenador,
        semAutenticacaoLocal,
        perfil: perfil || ""
    };
}

function podeGerenciarPontosFixos(req) {
    const permissoes = obterPermissoesUsuario(req);
    if (permissoes.has(MAPA_PONTOS_FIXOS_PERMISSION)) return true;

    const perfil = obterPerfilPontosFixos(req);
    return perfil.admin || perfil.coordenador || perfil.semAutenticacaoLocal;
}

function obterPermissoesPontosFixos(req) {
    const perfil = obterPerfilPontosFixos(req);

    return {
        canManage: podeGerenciarPontosFixos(req),
        canViewSensitive: perfil.admin || perfil.coordenador || perfil.semAutenticacaoLocal,
        permission: MAPA_PONTOS_FIXOS_PERMISSION,
        modo: perfil.semAutenticacaoLocal ? "sem autenticacao local" : "headers"
    };
}

function validarPermissaoPontosFixos(req, res) {
    if (podeGerenciarPontosFixos(req)) return true;

    res.status(403).json({
        erro: "Acesso restrito a admin/coordenador para gerenciar pontos fixos.",
        permissao: MAPA_PONTOS_FIXOS_PERMISSION
    });
    return false;
}

function responderErroPontoFixo(res, err, mensagemPadrao) {
    if (err instanceof MapaPontosFixosError) {
        return res.status(err.status || 400).json({ erro: err.message });
    }

    console.error(mensagemPadrao, err);
    return res.status(500).json({ erro: mensagemPadrao });
}

function obterFiltrosRequestCoordenadorRota(req = {}) {
    return normalizarFiltrosCoordenadorRota({
        ...(req.query || {}),
        ...(req.body || {})
    });
}

function filtrarPontosFixosPorEscopoCoordenadorRota(pontos, filtros) {
    if (!isEscopoInstalacaoCoordenadorRota(filtros)) return pontos;

    const tecnicosValidos = obterTecnicosValidosCoordenadorRota(filtros);

    return (pontos || []).filter(ponto => {
        const tecnicoId = ponto.tecnicoId || ponto.tecnico_id || "";
        return !tecnicoId || tecnicoPermitidoCoordenadorRota(tecnicoId, tecnicosValidos);
    });
}

function filtrarDesenhosPorEscopoCoordenadorRota(desenhos, filtros) {
    if (!isEscopoInstalacaoCoordenadorRota(filtros)) return desenhos;

    const tecnicosValidos = obterTecnicosValidosCoordenadorRota(filtros);

    return (desenhos || []).filter(desenho => {
        const tecnicosVinculados = desenho.tecnicosVinculados || desenho.tecnicos_vinculados_json || [];
        if (!tecnicosVinculados.length) return true;

        return tecnicosVinculados.some(tecnico => tecnicoPermitidoCoordenadorRota(tecnico, tecnicosValidos));
    });
}

function tecnicoVeiculoPermitidoCoordenadorRota(veiculo, tecnicosValidos) {
    const tecnicoId = veiculo?.tecnicoId || veiculo?.tecnico_id || "";
    const sugestaoTecnicoId = veiculo?.sugestaoVinculoTecnicoId || veiculo?.tecnicoDetectadoId || "";

    return tecnicoPermitidoCoordenadorRota(tecnicoId, tecnicosValidos)
        || tecnicoPermitidoCoordenadorRota(sugestaoTecnicoId, tecnicosValidos);
}

function montarResumoVeiculosRastreameCoordenadorRota(veiculos = [], fontes = []) {
    const nomesFontes = [...new Set([
        ...fontes.map(fonte => fonte.nome || fonte.fonte).filter(Boolean),
        ...veiculos.map(veiculo => veiculo.fonte || veiculo.empresaRastreame).filter(Boolean)
    ])];
    const porFonte = Object.fromEntries(nomesFontes.map(nome => [nome, 0]));

    veiculos.forEach(veiculo => {
        const fonte = veiculo.fonte || veiculo.empresaRastreame;
        if (fonte) porFonte[fonte] = (porFonte[fonte] || 0) + 1;
    });

    return {
        total: veiculos.length,
        porFonte,
        vinculados: veiculos.filter(veiculo => veiculo.tecnicoId || veiculo.tecnico_id).length,
        semVinculo: veiculos.filter(veiculo => !(veiculo.tecnicoId || veiculo.tecnico_id)).length,
        gpsTempoReal: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "tempo_real").length,
        gpsAtual: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "atual").length,
        gpsAntigo: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "antiga").length,
        gpsDesatualizado: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "desatualizada").length,
        semPosicao: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "sem_posicao").length
    };
}

function filtrarRastreamePorEscopoCoordenadorRota(dados = {}, filtros = {}) {
    if (!isEscopoInstalacaoCoordenadorRota(filtros)) return dados;

    const tecnicosValidos = obterTecnicosValidosCoordenadorRota(filtros);
    const tecnicos = (dados.tecnicos || [])
        .filter(tecnico => tecnicoPermitidoCoordenadorRota(tecnico.tecnicoId || tecnico.id, tecnicosValidos));
    const veiculos = (dados.veiculos || [])
        .filter(veiculo => tecnicoVeiculoPermitidoCoordenadorRota(veiculo, tecnicosValidos));
    const veiculosSemVinculo = veiculos.filter(veiculo => !(veiculo.tecnicoId || veiculo.tecnico_id));
    const resumo = montarResumoVeiculosRastreameCoordenadorRota(veiculos, dados.fontes || []);
    const fontes = (dados.fontes || []).map(fonte => {
        const nomeFonte = fonte.nome || fonte.fonte;
        const total = resumo.porFonte[nomeFonte] || 0;

        return {
            ...fonte,
            total,
            veiculosVinculados: veiculos.filter(veiculo => (veiculo.fonte || veiculo.empresaRastreame) === nomeFonte && (veiculo.tecnicoId || veiculo.tecnico_id)).length,
            semVinculo: veiculos.filter(veiculo => (veiculo.fonte || veiculo.empresaRastreame) === nomeFonte && !(veiculo.tecnicoId || veiculo.tecnico_id)).length
        };
    });

    return {
        ...dados,
        tecnicos,
        veiculos,
        veiculosSemVinculo,
        resumo,
        fontes
    };
}

async function listarPontosFixosCoordenadorRota(req = null) {
    const filtros = obterFiltrosRequestCoordenadorRota(req || {});
    const pontos = await listarPontosFixosMapa(obterPerfilPontosFixos(req), obterTecnicosValidosCoordenadorRota(filtros));
    return filtrarPontosFixosPorEscopoCoordenadorRota(pontos, filtros);
}

async function listarDesenhosCoordenadorRota(req = null, dataReferencia = null) {
    const filtros = obterFiltrosRequestCoordenadorRota(req || {});
    const data = dataReferencia || filtros.data || obterHojeBrasilBackend();
    const desenhos = await listarDesenhosMapa({
        dataReferencia: data,
        perfil: obterPerfilPontosFixos(req),
        tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros)
    });

    return filtrarDesenhosPorEscopoCoordenadorRota(desenhos, filtros);
}

async function emitirPontosFixosCoordenadorRota(req = null) {
    const pontos = await listarPontosFixosMapa(obterPerfilPontosFixos(req), obterTecnicosValidosCoordenadorRota());

    io.emit("coordenador-rota:pontos-fixos", {
        pontos,
        updatedAt: new Date().toISOString()
    });

    return pontos;
}

function podeGerenciarDesenhos(req) {
    const permissoes = obterPermissoesUsuario(req);
    if (permissoes.has(MAPA_DESENHOS_PERMISSION)) return true;

    const perfil = obterPerfilPontosFixos(req);
    return perfil.admin || perfil.coordenador || perfil.semAutenticacaoLocal;
}

function obterPermissoesDesenhos(req) {
    const perfil = obterPerfilPontosFixos(req);

    return {
        canManage: podeGerenciarDesenhos(req),
        canViewSensitive: perfil.admin || perfil.coordenador || perfil.semAutenticacaoLocal,
        permission: MAPA_DESENHOS_PERMISSION,
        modo: perfil.semAutenticacaoLocal ? "sem autenticacao local" : "headers"
    };
}

function validarPermissaoDesenhos(req, res) {
    if (podeGerenciarDesenhos(req)) return true;

    res.status(403).json({
        erro: "Acesso restrito a admin/coordenador para gerenciar desenhos do mapa.",
        permissao: MAPA_DESENHOS_PERMISSION
    });
    return false;
}

function responderErroDesenho(res, err, mensagemPadrao) {
    if (err instanceof MapaDesenhosError) {
        return res.status(err.status || 400).json({ erro: err.message });
    }

    console.error(mensagemPadrao, err);
    return res.status(500).json({ erro: mensagemPadrao });
}

async function emitirDesenhosCoordenadorRota(req = null, dataReferencia = null) {
    const data = dataReferencia || req?.query?.data || req?.body?.dataReferencia || req?.body?.data_referencia || obterHojeBrasilBackend();
    const desenhos = await listarDesenhosMapa({
        dataReferencia: data,
        perfil: obterPerfilPontosFixos(req),
        tecnicosValidos: obterTecnicosValidosCoordenadorRota()
    });

    io.emit("coordenador-rota:desenhos", {
        data,
        desenhos,
        updatedAt: new Date().toISOString()
    });

    return desenhos;
}

function obterPermissoesUsuario(req) {
    const valores = [
        req.headers?.["x-user-permissions"],
        req.headers?.["x-permissions"],
        req.headers?.["x-permissoes"],
        req.headers?.["x-user-permission"]
    ].filter(Boolean);

    return new Set(valores
        .flatMap(valor => String(valor).split(/[,\s;|]+/))
        .map(valor => valor.trim())
        .filter(Boolean));
}

function temContextoAutenticacaoAlmoco(req) {
    return Boolean(
        req.headers?.["x-user-role"]
        || req.headers?.["x-user-profile"]
        || req.headers?.["x-perfil"]
        || req.headers?.["x-user-permissions"]
        || req.headers?.["x-permissions"]
        || req.headers?.["x-permissoes"]
        || req.headers?.["x-user-permission"]
        || req.headers?.["x-user-id"]
        || req.headers?.["x-user-name"]
        || req.headers?.["x-usuario"]
    );
}

function podeGerenciarAlmoco(req) {
    const permissoes = obterPermissoesUsuario(req);
    if (permissoes.has(AGENDA_LUNCH_PERMISSION)) return true;

    const perfil = normalizarTextoRota(req.headers?.["x-user-role"] || req.headers?.["x-user-profile"] || req.headers?.["x-perfil"]);
    if (perfil && ["admin", "administrador"].some(permitido => perfil.includes(permitido))) return true;

    return !temContextoAutenticacaoAlmoco(req);
}

function obterPermissoesAlmoco(req) {
    return {
        canManage: podeGerenciarAlmoco(req),
        permission: AGENDA_LUNCH_PERMISSION,
        modo: temContextoAutenticacaoAlmoco(req) ? "headers" : "sem autenticacao local"
    };
}

function validarPermissaoAlmoco(req, res) {
    if (podeGerenciarAlmoco(req)) return true;

    res.status(403).json({
        erro: "Você não possui permissão para gerenciar bloqueios de horário.",
        permissao: AGENDA_LUNCH_PERMISSION
    });
    return false;
}

function obterUsuarioAcaoAlmoco(req) {
    return String(
        req.headers?.["x-user-id"]
        || req.headers?.["x-user-name"]
        || req.headers?.["x-usuario"]
        || "agenda-tecnicos"
    ).trim();
}

function obterRotuloBloqueioAgenda(item = {}) {
    const tipo = String(item.tipo || "almoco").trim().toLowerCase();
    const rotulos = {
        almoco: "Almoço",
        compromisso: "Compromisso",
        reuniao: "Reunião",
        treinamento: "Treinamento",
        indisponivel: "Indisponível",
        outro: "Outro"
    };

    return rotulos[tipo] || "Bloqueio";
}

function clonarSnapshotAuditoria(valor) {
    if (valor === undefined || valor === null) return null;

    try {
        return JSON.parse(JSON.stringify(valor));
    } catch {
        return valor;
    }
}

async function emitirAlmocosAgenda(data) {
    const almocos = await listarAlmocosAgenda(data, obterTecnicosValidos());

    io.emit("agendaTecnicosAlmocosUpdate", {
        data,
        almocos
    });

    return almocos;
}

function responderErroAlmoco(res, err, mensagemPadrao) {
    if (err instanceof AgendaLunchError) {
        return res.status(err.status || 400).json({ erro: err.message });
    }

    console.error(mensagemPadrao, err);
    return res.status(500).json({ erro: mensagemPadrao });
}

app.post("/api/agenda-tecnicos/ordens-servico/:osId/reagendar", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const osId = String(req.params.osId || "").trim();
        const agendamento = normalizarAgendamentoSgp({
            data: req.body?.data,
            hora: req.body?.hora
        });
        const tecnicoDestino = String(req.body?.tecnico || req.body?.tecnicoDestino || "").trim();
        const tecnicoResponsavelSgpId = String(
            req.body?.tecnicoResponsavelSgpId
            || req.body?.tecnico_responsavel_sgp_id
            || obterResponsavelSgpPorTecnicoAgenda(tecnicoDestino)
            || ""
        ).trim();

        if (!osId) {
            return res.status(400).json({ erro: "Informe o ID da ordem de serviço." });
        }

        if (!agendamento) {
            return res.status(400).json({ erro: "Informe data e hora válidas para o reagendamento." });
        }

        if (!tecnicoDestino) {
            return res.status(400).json({ erro: "Informe o técnico de destino." });
        }

        if (!tecnicoResponsavelSgpId) {
            return res.status(400).json({ erro: "Técnico de destino sem usuário SGP vinculado." });
        }

        const resultadoSgp = await atualizarAgendamentoChamadoSgp({
            osId,
            agendamento,
            tecnicoResponsavelSgpId
        });

        limparCachesAgendaSgp(req.body?.dataAnterior || req.body?.data_origem);
        limparCachesAgendaSgp(agendamento.data);

        registrarLogCoordenadorRota("reagendar_os_sgp", {
            osId,
            tecnicoDestino,
            tecnicoResponsavelSgpId,
            dataAnterior: req.body?.dataAnterior || req.body?.data_origem || "",
            horaAnterior: req.body?.horaAnterior || req.body?.hora_origem || "",
            dataDestino: agendamento.data,
            horaDestino: agendamento.hora
        }, req);

        const tecnicoAnterior = String(req.body?.tecnicoAnterior || req.body?.tecnico_origem || "").trim();
        const dataAnterior = String(req.body?.dataAnterior || req.body?.data_origem || "").trim();
        const horaAnterior = String(req.body?.horaAnterior || req.body?.hora_origem || "").trim().slice(0, 5);
        const tecnicoMudou = Boolean(tecnicoAnterior && tecnicoAnterior !== tecnicoDestino);

        registrarAuditoriaOperacional({
            acao: tecnicoMudou ? "TRANSFERIR_CHAMADO" : "REAGENDAR_CHAMADO",
            entidade: "chamado",
            entidade_id: osId,
            tipo: "manutencao",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_anterior: {
                tecnico_nome: tecnicoAnterior || null,
                data: dataAnterior || null,
                horario: horaAnterior || null
            },
            valor_novo: {
                tecnico_nome: tecnicoDestino,
                tecnico_responsavel_sgp_id: tecnicoResponsavelSgpId,
                data: agendamento.data,
                horario: agendamento.hora,
                data_hora: agendamento.dataHora
            },
            metadata: {
                sgp: true,
                resultado_sgp: resultadoSgp,
                rota: "ordens-servico/reagendar"
            }
        }, req);

        res.json({
            sucesso: true,
            osId,
            data: agendamento.data,
            hora: agendamento.hora,
            dataAgendamento: agendamento.dataHora,
            tecnico: tecnicoDestino,
            tecnicoResponsavelSgpId,
            sgp: resultadoSgp
        });
    } catch (err) {
        console.error("Erro ao reagendar OS no SGP:", err.message);
        res.status(err.statusCode || 500).json({ erro: err.message || "Não foi possível reagendar a OS no SGP." });
    }
});

app.get("/api/agenda-tecnicos/massivos/historico", async (req, res) => {
    try {
        const periodo = normalizarPeriodoMassivosHistorico(req.query);
        const dados = await montarHistoricoMassivos(periodo, {
            force: req.query.force === "1" || req.query.force === "true",
            incluirAtivos: req.query.incluirAtivos === "1" || req.query.incluirAtivos === "true",
            incluirCriados: req.query.incluirCriados === "1" || req.query.incluirCriados === "true"
        });

        res.json(dados);
    } catch (err) {
        const statusCode = Number(err.statusCode) || 500;
        if (statusCode >= 500) {
            console.error("Erro ao buscar histórico de massivos:", err.message);
        }

        res.status(statusCode).json({
            erro: err.message || "Não foi possível buscar o histórico de massivos."
        });
    }
});

app.get("/api/agenda-tecnicos/impacto-bairros", async (req, res) => {
    try {
        const periodo = normalizarPeriodoImpactoBairros(req.query);
        const dados = await montarImpactoBairrosPeriodo(periodo, {
            force: req.query.force === "1" || req.query.force === "true",
            includePrevious: req.query.includePrevious !== "0" && req.query.includePrevious !== "false"
        });

        res.json(dados);
    } catch (err) {
        const statusCode = Number(err.statusCode) || 500;
        if (statusCode >= 500) {
            console.error("Erro ao buscar impacto por bairros:", err.message);
        }

        res.status(statusCode).json({
            erro: err.message || "Não foi possível buscar o impacto por bairros."
        });
    }
});

app.get("/api/agenda-tecnicos/almocos", async (req, res) => {
    try {
        const data = String(req.query.data || "").trim();
        const almocos = await listarAlmocosAgenda(data, obterTecnicosValidos());

        res.json({
            data,
            almocos,
            permissoes: obterPermissoesAlmoco(req)
        });
    } catch (err) {
        responderErroAlmoco(res, err, "Não foi possível buscar os bloqueios de horário.");
    }
});

app.post("/api/agenda-tecnicos/almocos", async (req, res) => {
    if (!validarPermissaoAlmoco(req, res)) return;

    try {
        const almoco = await criarAlmocoAgenda(req.body || {}, {
            tecnicosValidos: obterTecnicosValidos(),
            criadoPor: obterUsuarioAcaoAlmoco(req)
        });
        const almocos = await emitirAlmocosAgenda(almoco.data);

        registrarAuditoriaOperacional({
            acao: "CRIAR_AUSENCIA",
            entidade: "ausencia",
            entidade_id: almoco.id,
            tipo: "ausencias",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_novo: {
                ...almoco,
                tipo: obterRotuloBloqueioAgenda(almoco)
            },
            metadata: {
                modulo: "agenda-tecnicos/almocos",
                tecnico_id: almoco.tecnico_id || almoco.tecnicoId
            }
        }, req);

        res.status(201).json({
            almoco,
            almocos,
            data: almoco.data,
            permissoes: obterPermissoesAlmoco(req)
        });
    } catch (err) {
        responderErroAlmoco(res, err, "Não foi possível salvar o bloqueio de horário.");
    }
});

app.put("/api/agenda-tecnicos/almocos/:id", async (req, res) => {
    if (!validarPermissaoAlmoco(req, res)) return;

    try {
        let almocoAnterior = null;
        try {
            const dataReferencia = req.body?.data || req.query?.data || "";
            if (dataReferencia) {
                const almocosAtuais = await listarAlmocosAgenda(dataReferencia, obterTecnicosValidos());
                almocoAnterior = almocosAtuais.find(item => Number(item.id) === Number(req.params.id)) || null;
            }
        } catch {
            almocoAnterior = null;
        }

        const almoco = await atualizarAlmocoAgenda(req.params.id, req.body || {}, {
            tecnicosValidos: obterTecnicosValidos()
        });
        const almocos = await emitirAlmocosAgenda(almoco.data);

        registrarAuditoriaOperacional({
            acao: "ALTERAR_AUSENCIA",
            entidade: "ausencia",
            entidade_id: almoco.id,
            tipo: "ausencias",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_anterior: almocoAnterior ? { ...almocoAnterior, tipo: obterRotuloBloqueioAgenda(almocoAnterior) } : null,
            valor_novo: {
                ...almoco,
                tipo: obterRotuloBloqueioAgenda(almoco)
            },
            metadata: {
                modulo: "agenda-tecnicos/almocos",
                tecnico_id: almoco.tecnico_id || almoco.tecnicoId
            }
        }, req);

        res.json({
            almoco,
            almocos,
            data: almoco.data,
            permissoes: obterPermissoesAlmoco(req)
        });
    } catch (err) {
        responderErroAlmoco(res, err, "Não foi possível atualizar o bloqueio de horário.");
    }
});

app.delete("/api/agenda-tecnicos/almocos/:id", async (req, res) => {
    if (!validarPermissaoAlmoco(req, res)) return;

    try {
        const almocoRemovido = await removerAlmocoAgenda(req.params.id, obterTecnicosValidos());
        const almocos = await emitirAlmocosAgenda(almocoRemovido.data);

        registrarAuditoriaOperacional({
            acao: "REMOVER_AUSENCIA",
            entidade: "ausencia",
            entidade_id: almocoRemovido.id,
            tipo: "ausencias",
            nivel: "critical",
            origem: "AGENDA_TECNICOS",
            valor_anterior: {
                ...almocoRemovido,
                tipo: obterRotuloBloqueioAgenda(almocoRemovido)
            },
            valor_novo: null,
            metadata: {
                modulo: "agenda-tecnicos/almocos",
                tecnico_id: almocoRemovido.tecnico_id || almocoRemovido.tecnicoId
            }
        }, req);

        res.json({
            almoco: null,
            removido: almocoRemovido,
            almocos,
            data: almocoRemovido.data,
            permissoes: obterPermissoesAlmoco(req)
        });
    } catch (err) {
        responderErroAlmoco(res, err, "Não foi possível remover o bloqueio de horário.");
    }
});

app.get("/api/agenda-tecnicos/chat-interno/usuarios", async (req, res) => {
    try {
        const usuarios = await listarUsuariosChatInterno();
        res.json({
            usuarios,
            atualizadoEm: new Date(chatInternoUsuariosCache.carregadoEm || Date.now()).toISOString()
        });
    } catch (err) {
        console.error("Erro ao buscar usuários do chat interno:", err.message);
        res.status(502).json({ erro: "Não foi possível buscar usuários do chat interno." });
    }
});

app.get("/api/tecnico-responsaveis-servicos", (req, res) => {
    try {
        res.json({ responsaveis: listarResponsaveisServicos() });
    } catch (err) {
        console.error("Erro ao buscar responsáveis dos técnicos:", err.message);
        res.status(500).json({ erro: "Não foi possível buscar os responsáveis." });
    }
});

app.put("/api/tecnico-responsaveis-servicos/:tecnicoId", (req, res) => {
    try {
        const tecnicoId = req.params.tecnicoId;
        const responsavelNome = String(req.body?.responsavel_nome || "").trim();

        if (!validarTecnicoServico(tecnicoId)) {
            return res.status(404).json({ erro: "Técnico não encontrado." });
        }

        if (!responsavelNome || responsavelNome === "Sem responsável") {
            removerResponsavelServico(tecnicoId);
            return res.json({ responsavel: null, responsaveis: listarResponsaveisServicos() });
        }

        if (!RESPONSAVEIS_SERVICOS_OPCOES.includes(responsavelNome)) {
            return res.status(400).json({ erro: "Responsável inválido." });
        }

        const store = lerStoreResponsaveisServicos();
        let item = store.items.find(registro => registro.tecnico_id === tecnicoId);
        const updatedAt = new Date().toISOString();

        if (!item) {
            item = {
                id: store.nextId,
                tecnico_id: tecnicoId,
                responsavel_nome: responsavelNome,
                updated_at: updatedAt
            };
            store.nextId += 1;
            store.items.push(item);
        } else {
            item.responsavel_nome = responsavelNome;
            item.updated_at = updatedAt;
        }

        salvarStoreResponsaveisServicos(store);
        emitirResponsaveisServicos();
        res.json({ responsavel: item, responsaveis: listarResponsaveisServicos() });
    } catch (err) {
        console.error("Erro ao salvar responsável do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível salvar o responsável." });
    }
});

app.delete("/api/tecnico-responsaveis-servicos/:tecnicoId", (req, res) => {
    try {
        const tecnicoId = req.params.tecnicoId;

        if (!validarTecnicoServico(tecnicoId)) {
            return res.status(404).json({ erro: "Técnico não encontrado." });
        }

        removerResponsavelServico(tecnicoId);
        res.json({ responsavel: null, responsaveis: listarResponsaveisServicos() });
    } catch (err) {
        console.error("Erro ao remover responsável do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível remover o responsável." });
    }
});

function responderErroAgendaTecnicosHorarios(res, err, fallback = "Não foi possível salvar os horários da semana.") {
    if (err instanceof TechnicianScheduleError) {
        return res.status(err.status || 400).json({ erro: err.message });
    }

    console.error(fallback, err.message);
    return res.status(500).json({ erro: fallback });
}

async function emitirHorariosSemanaAgendaTecnicos(weekStart) {
    try {
        const schedule = await listarHorariosSemana({
            weekStart,
            tecnicosValidos: obterTecnicosValidosHorariosSemana()
        });
        const scheduleAgendaTecnica = filtrarHorarioTecnicosAgendaTecnica(schedule);

        io.emit("agendaTecnicosHorariosSemanaUpdate", scheduleAgendaTecnica);
        io.emit("tecnicoHorariosUpdate", {
            horarios: scheduleAgendaTecnica.shifts.map(item => ({
                id: item.id,
                entrada: item.startTime,
                saida: item.endTime,
                nome: item.name,
                ativo: item.active !== false
            })),
            schedule: scheduleAgendaTecnica
        });
        return scheduleAgendaTecnica;
    } catch (err) {
        console.error("Erro ao emitir horários da semana:", err.message);
        return null;
    }
}

app.get("/api/agenda-tecnicos/tecnicos", (req, res) => {
    try {
        const tecnicosVisiveis = [...obterTecnicosValidosAgendaTecnica()].sort(compararTecnicosAgendaTecnica);
        const tecnicosVisiveisSet = new Set(tecnicosVisiveis);
        res.json({
            tecnicos: tecnicosVisiveis,
            cadastro: obterCadastroTecnicos({ incluirInstalacaoSemGps: true })
                .filter(tecnico => tecnicosVisiveisSet.has(tecnico.id))
        });
    } catch (err) {
        console.error("Erro ao buscar técnicos:", err.message);
        res.status(500).json({ erro: "Não foi possível buscar os técnicos." });
    }
});

async function montarResumoChamadosSimultaneosAgendaTecnica(data) {
    const dataNormalizada = normalizarDataAusencia(data) || obterHojeBrasilBackend();
    const resultado = await buscarAgenda(dataNormalizada);
    const agendaEnriquecida = await enriquecerAgendaComContratos(resultado.ordensAgenda, resultado.contratosPendentes, {
        dataFiltro: dataNormalizada
    });
    const agenda = filtrarAgendaMontadaAgendaTecnica(agendaEnriquecida || resultado.agenda);
    const tecnicosValidos = obterTecnicosValidosAgendaTecnica();
    const chamados = montarChamadosCoordenadorRota(agenda, dataNormalizada)
        .filter(chamado => tecnicosValidos.has(chamado.tecnicoAtualId));

    return montarResumoChamadosSimultaneosRota(chamados, dataNormalizada);
}

app.get("/api/agenda-tecnicos/chamados-simultaneos", async (req, res) => {
    try {
        const data = normalizarDataAusencia(req.query.data) || obterHojeBrasilBackend();
        const chamadosSimultaneos = await montarResumoChamadosSimultaneosAgendaTecnica(data);

        res.json({
            data,
            chamadosSimultaneos
        });
    } catch (err) {
        console.error("Erro ao buscar chamados simultâneos:", err.message);
        res.status(500).json({ erro: "Não foi possível buscar os chamados simultâneos." });
    }
});

app.post("/api/agenda-tecnicos/chamados-simultaneos/principal", async (req, res) => {
    try {
        const data = normalizarDataAusencia(req.body?.data) || obterHojeBrasilBackend();
        const tecnicoId = req.body?.tecnico_id || req.body?.tecnicoId || req.body?.tecnico;
        const chamadoId = req.body?.chamado_id || req.body?.chamadoId || req.body?.chamado;
        const motivo = String(req.body?.motivo || "").trim();
        const chamadosEnvolvidos = Array.isArray(req.body?.chamados_envolvidos || req.body?.chamadosEnvolvidos)
            ? (req.body.chamados_envolvidos || req.body.chamadosEnvolvidos)
            : [];
        const correcao = registrarChamadoPrincipalSimultaneo({
            data,
            tecnicoId,
            chamadoId,
            motivo,
            chamadosEnvolvidos,
            req
        });
        let chamadosSimultaneos = null;

        try {
            chamadosSimultaneos = await montarResumoChamadosSimultaneosAgendaTecnica(data);
        } catch (err) {
            console.warn("Não foi possível recalcular chamados simultâneos após correção:", err.message);
        }

        io.emit("agendaTecnicosChamadosSimultaneosUpdate", {
            data,
            correcao,
            chamadosSimultaneos
        });

        res.json({
            ok: true,
            correcao,
            chamadosSimultaneos
        });
    } catch (err) {
        console.error("Erro ao definir chamado principal simultâneo:", err.message);
        res.status(err.statusCode || 500).json({ erro: err.message || "Não foi possível definir o chamado principal." });
    }
});

app.get("/api/agenda-tecnicos/horarios-semana", async (req, res) => {
    try {
        const weekStart = normalizarDataAgenda(req.query.weekStart) || req.query.weekStart || null;
        const schedule = await listarHorariosSemana({
            weekStart,
            tecnicosValidos: obterTecnicosValidosHorariosSemana()
        });

        res.json(filtrarHorarioTecnicosAgendaTecnica(schedule));
    } catch (err) {
        responderErroAgendaTecnicosHorarios(res, err, "Não foi possível buscar os horários da semana.");
    }
});

app.put("/api/agenda-tecnicos/horarios-semana", async (req, res) => {
    try {
        const weekStartAuditoria = normalizarDataAgenda(req.body?.weekStart) || req.body?.weekStart || null;
        const scheduleAnterior = await listarHorariosSemana({
            weekStart: weekStartAuditoria,
            tecnicosValidos: obterTecnicosValidosHorariosSemana()
        }).catch(() => null);
        const schedule = await salvarHorariosSemana(req.body || {}, {
            tecnicosValidos: obterTecnicosValidosHorariosSemana()
        });

        await emitirHorariosSemanaAgendaTecnicos(schedule.weekStart);

        registrarAuditoriaOperacional({
            acao: "ALTERAR_AGENDA",
            entidade: "agenda",
            entidade_id: schedule.weekStart,
            tipo: "agenda",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_anterior: scheduleAnterior,
            valor_novo: schedule,
            metadata: {
                modulo: "agenda-tecnicos/horarios-semana",
                weekStart: schedule.weekStart
            }
        }, req);

        res.json(filtrarHorarioTecnicosAgendaTecnica(schedule));
    } catch (err) {
        responderErroAgendaTecnicosHorarios(res, err);
    }
});

app.delete("/api/agenda-tecnicos/horarios-semana", async (req, res) => {
    try {
        const weekStartAuditoria = req.query.weekStart || req.body?.weekStart || null;
        const scheduleAnterior = await listarHorariosSemana({
            weekStart: weekStartAuditoria,
            tecnicosValidos: obterTecnicosValidosHorariosSemana()
        }).catch(() => null);
        const schedule = await restaurarConfiguracaoSemana(req.query.weekStart || req.body?.weekStart, {
            tecnicosValidos: obterTecnicosValidosHorariosSemana()
        });

        await emitirHorariosSemanaAgendaTecnicos(schedule.weekStart);

        registrarAuditoriaOperacional({
            acao: "ALTERAR_AGENDA",
            entidade: "agenda",
            entidade_id: schedule.weekStart,
            tipo: "agenda",
            nivel: "critical",
            origem: "AGENDA_TECNICOS",
            valor_anterior: scheduleAnterior,
            valor_novo: schedule,
            metadata: {
                modulo: "agenda-tecnicos/horarios-semana",
                restauracao: true,
                weekStart: schedule.weekStart
            }
        }, req);

        res.json(filtrarHorarioTecnicosAgendaTecnica(schedule));
    } catch (err) {
        responderErroAgendaTecnicosHorarios(res, err, "Não foi possível restaurar a configuração da semana.");
    }
});

app.get("/api/tecnico-horarios", async (req, res) => {
    try {
        res.json({ horarios: await listarTurnosLegado({ tecnicosValidos: obterTecnicosValidosHorariosSemana() }) });
    } catch (err) {
        responderErroAgendaTecnicosHorarios(res, err, "Não foi possível buscar os horários dos técnicos.");
    }
});

app.put("/api/tecnico-horarios/:horarioId", async (req, res) => {
    try {
        const entrada = normalizarHoraAgendaTecnico(req.body?.entrada);
        const saida = normalizarHoraAgendaTecnico(req.body?.saida);

        if (!entrada || !saida) {
            return res.status(400).json({ erro: "Informe entrada e saída no formato HH:MM." });
        }

        if (entrada === saida) {
            return res.status(400).json({ erro: "A saída não pode ser igual à entrada." });
        }

        const turnosAntes = await listarTurnosLegado({ tecnicosValidos: obterTecnicosValidosHorariosSemana() }).catch(() => []);
        const turnoAnterior = turnosAntes.find(item => String(item.id) === String(req.params.horarioId)) || null;
        const resultado = await atualizarTurnoLegado(req.params.horarioId, {
            entrada,
            saida
        }, { tecnicosValidos: obterTecnicosValidos() });

        await emitirHorariosSemanaAgendaTecnicos();

        registrarAuditoriaOperacional({
            acao: "ALTERAR_HORARIO_FIXO",
            entidade: "horario",
            entidade_id: req.params.horarioId,
            tipo: "agenda",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_anterior: turnoAnterior,
            valor_novo: resultado,
            metadata: {
                modulo: "tecnico-horarios",
                horario_id: req.params.horarioId
            }
        }, req);

        res.json(resultado);
    } catch (err) {
        responderErroAgendaTecnicosHorarios(res, err, "Não foi possível salvar o horário dos técnicos.");
    }
});

app.get("/api/technician-absences", (req, res) => {
    try {
        const absenceDate = req.query.date ? normalizarDataAusencia(req.query.date) : "";

        if (req.query.date && !absenceDate) {
            return res.status(400).json({ erro: "Informe uma data válida no formato YYYY-MM-DD." });
        }

        const absences = listarAusenciasTecnicosPorData(absenceDate);
        res.json({ date: absenceDate || null, absences, ausencias: absences });
    } catch (err) {
        console.error("Erro ao buscar ausências dos técnicos:", err.message);
        res.status(500).json({ erro: "Não foi possível buscar as ausências." });
    }
});

app.post("/api/technician-absences", (req, res) => {
    try {
        const payload = montarPayloadAusencias(req);
        const erroValidacao = validarPayloadAusencia(payload);

        if (erroValidacao) {
            return res.status(400).json({ erro: erroValidacao });
        }

        const store = lerStoreAusenciasTecnicos();
        const updatedAt = new Date().toISOString();
        const usuarioAcao = obterUsuarioAcaoAusencia(req);
        let item = store.items.find(registro => (
            registro.technician_id === payload.technicianId &&
            !isAusenciaCanceladaOuInativa(registro)
        ));
        const itemAnterior = item ? clonarSnapshotAuditoria(item) : null;
        const acaoAuditoria = item ? "ALTERAR_AUSENCIA" : "CRIAR_AUSENCIA";

        if (!item) {
            item = {
                id: store.nextId,
                technician_id: payload.technicianId,
                absence_date: payload.absenceDate,
                absence_reason: payload.absenceReason,
                absence_note: payload.absenceNote,
                active: true,
                created_by: usuarioAcao,
                updated_by: usuarioAcao,
                created_at: updatedAt,
                updated_at: updatedAt
            };
            store.nextId += 1;
            store.items.push(item);
        } else {
            item.absence_date = payload.absenceDate;
            item.absence_reason = payload.absenceReason;
            item.absence_note = payload.absenceNote;
            item.active = true;
            item.updated_by = usuarioAcao;
            delete item.deleted_at;
            delete item.deleted_by;
            item.updated_at = updatedAt;
        }

        salvarStoreAusenciasTecnicos(store);
        emitirAusenciasTecnicos();

        const absences = listarAusenciasTecnicosAtivas();
        res.json({
            absence: item,
            absences,
            ausencias: absences
        });

        registrarAuditoriaOperacional({
            acao: acaoAuditoria,
            entidade: "ausencia",
            entidade_id: item.id,
            tipo: "ausencias",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_anterior: itemAnterior,
            valor_novo: clonarSnapshotAuditoria(item),
            metadata: {
                modulo: "technician-absences",
                technician_id: item.technician_id,
                absence_reason: item.absence_reason
            }
        }, req);
    } catch (err) {
        console.error("Erro ao salvar ausência do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível salvar a ausência." });
    }
});

app.put("/api/technician-absences/:id", (req, res) => {
    try {
        const id = Number(req.params.id);
        const store = lerStoreAusenciasTecnicos();
        const item = store.items.find(registro => (
            registro.id === id &&
            !isAusenciaCanceladaOuInativa(registro)
        ));

        if (!item) {
            return res.status(404).json({ erro: "Ausência não encontrada." });
        }

        const payload = montarPayloadAusencias(req, item);
        const erroValidacao = validarPayloadAusencia(payload);

        if (erroValidacao) {
            return res.status(400).json({ erro: erroValidacao });
        }

        const duplicado = store.items.find(registro => (
            registro.id !== id &&
            registro.technician_id === payload.technicianId &&
            !isAusenciaCanceladaOuInativa(registro)
        ));

        if (duplicado) {
            return res.status(409).json({ erro: "Já existe ausência para esse técnico." });
        }

        const usuarioAcao = obterUsuarioAcaoAusencia(req);
        const itemAnterior = clonarSnapshotAuditoria(item);
        item.technician_id = payload.technicianId;
        item.absence_date = payload.absenceDate;
        item.absence_reason = payload.absenceReason;
        item.absence_note = payload.absenceNote;
        item.active = true;
        item.updated_by = usuarioAcao;
        delete item.deleted_at;
        delete item.deleted_by;
        item.updated_at = new Date().toISOString();

        salvarStoreAusenciasTecnicos(store);
        emitirAusenciasTecnicos();

        const absences = listarAusenciasTecnicosAtivas();
        res.json({
            absence: item,
            absences,
            ausencias: absences
        });

        registrarAuditoriaOperacional({
            acao: "ALTERAR_AUSENCIA",
            entidade: "ausencia",
            entidade_id: item.id,
            tipo: "ausencias",
            nivel: "important",
            origem: "AGENDA_TECNICOS",
            valor_anterior: itemAnterior,
            valor_novo: clonarSnapshotAuditoria(item),
            metadata: {
                modulo: "technician-absences",
                technician_id: item.technician_id,
                absence_reason: item.absence_reason
            }
        }, req);
    } catch (err) {
        console.error("Erro ao atualizar ausência do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível atualizar a ausência." });
    }
});

app.delete("/api/technician-absences/:id", (req, res) => {
    try {
        const id = Number(req.params.id);
        const store = lerStoreAusenciasTecnicos();
        const item = store.items.find(registro => (
            registro.id === id &&
            !isAusenciaCanceladaOuInativa(registro)
        ));

        if (!item) {
            return res.status(404).json({ erro: "Ausência não encontrada." });
        }

        const itemAnterior = clonarSnapshotAuditoria(item);
        const updatedAt = new Date().toISOString();
        item.active = false;
        item.deleted_at = updatedAt;
        item.deleted_by = obterUsuarioAcaoAusencia(req);
        item.updated_at = updatedAt;

        salvarStoreAusenciasTecnicos(store);
        emitirAusenciasTecnicos();

        const absences = listarAusenciasTecnicosAtivas();
        res.json({
            absence: null,
            absences,
            ausencias: absences
        });

        registrarAuditoriaOperacional({
            acao: "REMOVER_AUSENCIA",
            entidade: "ausencia",
            entidade_id: item.id,
            tipo: "ausencias",
            nivel: "critical",
            origem: "AGENDA_TECNICOS",
            valor_anterior: itemAnterior,
            valor_novo: clonarSnapshotAuditoria(item),
            metadata: {
                modulo: "technician-absences",
                technician_id: item.technician_id,
                absence_reason: item.absence_reason
            }
        }, req);
    } catch (err) {
        console.error("Erro ao remover ausência do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível remover a ausência." });
    }
});

app.get("/api/technician-team-colors", (req, res) => {
    try {
        const teamDate = normalizarDataEquipe(req.query.date);

        if (!teamDate) {
            return res.status(400).json({ erro: "Informe uma data válida no formato YYYY-MM-DD." });
        }

        const teamColors = listarCoresEquipeTecnicosPorData(teamDate);
        res.json({ teamColors, coresEquipe: teamColors });
    } catch (err) {
        console.error("Erro ao buscar cores de equipe dos técnicos:", err.message);
        res.status(500).json({ erro: "Não foi possível buscar as cores de equipe." });
    }
});

app.post("/api/technician-team-colors", (req, res) => {
    try {
        const payload = montarPayloadCorEquipe(req);
        const erroValidacao = validarPayloadCorEquipe(payload);

        if (erroValidacao) {
            return res.status(400).json({ erro: erroValidacao });
        }

        const store = lerStoreCoresEquipeTecnicos();
        const updatedAt = new Date().toISOString();
        let item = store.items.find(registro => (
            registro.technician_id === payload.technicianId &&
            registro.team_date === payload.teamDate
        ));

        if (!item) {
            item = {
                id: store.nextId,
                technician_id: payload.technicianId,
                team_date: payload.teamDate,
                team_color: payload.teamColor,
                created_at: updatedAt,
                updated_at: updatedAt
            };
            store.nextId += 1;
            store.items.push(item);
        } else {
            item.team_color = payload.teamColor;
            item.updated_at = updatedAt;
        }

        salvarStoreCoresEquipeTecnicos(store);
        emitirCoresEquipeTecnicos(payload.teamDate);

        res.json({
            teamColor: item,
            teamColors: listarCoresEquipeTecnicosPorData(payload.teamDate)
        });
    } catch (err) {
        console.error("Erro ao salvar cor de equipe do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível salvar a cor de equipe." });
    }
});

app.put("/api/technician-team-colors/:id", (req, res) => {
    try {
        const id = Number(req.params.id);
        const store = lerStoreCoresEquipeTecnicos();
        const item = store.items.find(registro => registro.id === id);

        if (!item) {
            return res.status(404).json({ erro: "Cor de equipe não encontrada." });
        }

        const payload = montarPayloadCorEquipe(req, item);
        const erroValidacao = validarPayloadCorEquipe(payload);

        if (erroValidacao) {
            return res.status(400).json({ erro: erroValidacao });
        }

        const duplicado = store.items.find(registro => (
            registro.id !== id &&
            registro.technician_id === payload.technicianId &&
            registro.team_date === payload.teamDate
        ));

        if (duplicado) {
            return res.status(409).json({ erro: "Já existe cor de equipe para esse técnico nessa data." });
        }

        item.technician_id = payload.technicianId;
        item.team_date = payload.teamDate;
        item.team_color = payload.teamColor;
        item.updated_at = new Date().toISOString();

        salvarStoreCoresEquipeTecnicos(store);
        emitirCoresEquipeTecnicos(payload.teamDate);

        res.json({
            teamColor: item,
            teamColors: listarCoresEquipeTecnicosPorData(payload.teamDate)
        });
    } catch (err) {
        console.error("Erro ao atualizar cor de equipe do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível atualizar a cor de equipe." });
    }
});

app.delete("/api/technician-team-colors/:id", (req, res) => {
    try {
        const id = Number(req.params.id);
        const store = lerStoreCoresEquipeTecnicos();
        const item = store.items.find(registro => registro.id === id);

        if (!item) {
            return res.status(404).json({ erro: "Cor de equipe não encontrada." });
        }

        store.items = store.items.filter(registro => registro.id !== id);
        salvarStoreCoresEquipeTecnicos(store);
        emitirCoresEquipeTecnicos(item.team_date);

        res.json({
            teamColor: null,
            teamColors: listarCoresEquipeTecnicosPorData(item.team_date)
        });
    } catch (err) {
        console.error("Erro ao remover cor de equipe do técnico:", err.message);
        res.status(500).json({ erro: "Não foi possível remover a cor de equipe." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota", async (req, res) => {
    try {
        const dados = await montarDadosCoordenadorRota(req.query, req);
        res.json(dados);
    } catch (err) {
        console.error("Erro ao montar coordenador de rota:", err.message);
        res.status(500).json({ erro: "Não foi possível carregar o coordenador de rota." });
    }
});

app.get("/api/agenda-tecnicos/mapa-chamados", async (req, res) => {
    try {
        const dados = await montarDadosMapaChamados(req.query, req);
        res.json(dados);
    } catch (err) {
        console.error("Erro ao montar mapa de chamados:", err.message);
        res.status(500).json({ erro: "Não foi possível carregar o mapa de chamados." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/pontos-fixos", async (req, res) => {
    try {
        const pontos = await listarPontosFixosCoordenadorRota(req);
        res.json({
            pontos,
            pontosFixos: pontos,
            permissoes: obterPermissoesPontosFixos(req),
            tipos: obterTiposPontosFixos(),
            visibilidades: obterVisibilidadesPontosFixos()
        });
    } catch (err) {
        responderErroPontoFixo(res, err, "Não foi possível listar os pontos fixos.");
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/pontos-fixos", async (req, res) => {
    if (!validarPermissaoPontosFixos(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const ponto = await criarPontoFixoMapa(req.body || {}, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            criadoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("ponto_fixo_criar", {
            pontoId: ponto.id,
            nome: ponto.nome,
            tipo: ponto.tipo,
            tecnicoId: ponto.tecnicoId || null
        }, req);

        await emitirPontosFixosCoordenadorRota(req);
        const pontos = await listarPontosFixosCoordenadorRota(req);
        res.status(201).json({ ponto, pontos });
    } catch (err) {
        responderErroPontoFixo(res, err, "Não foi possível criar o ponto fixo.");
    }
});

app.put("/api/agenda-tecnicos/coordenador-rota/pontos-fixos/:id", async (req, res) => {
    if (!validarPermissaoPontosFixos(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const ponto = await atualizarPontoFixoMapa(req.params.id, req.body || {}, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            atualizadoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("ponto_fixo_editar", {
            pontoId: ponto.id,
            nome: ponto.nome,
            tipo: ponto.tipo,
            tecnicoId: ponto.tecnicoId || null
        }, req);

        await emitirPontosFixosCoordenadorRota(req);
        const pontos = await listarPontosFixosCoordenadorRota(req);
        res.json({ ponto, pontos });
    } catch (err) {
        responderErroPontoFixo(res, err, "Não foi possível editar o ponto fixo.");
    }
});

app.delete("/api/agenda-tecnicos/coordenador-rota/pontos-fixos/:id", async (req, res) => {
    if (!validarPermissaoPontosFixos(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const ponto = await removerPontoFixoMapa(req.params.id, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            removidoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("ponto_fixo_remover", {
            pontoId: ponto.id,
            nome: ponto.nome,
            tipo: ponto.tipo,
            tecnicoId: ponto.tecnicoId || null
        }, req);

        await emitirPontosFixosCoordenadorRota(req);
        const pontos = await listarPontosFixosCoordenadorRota(req);
        res.json({ ponto, pontos });
    } catch (err) {
        responderErroPontoFixo(res, err, "Não foi possível remover o ponto fixo.");
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/desenhos", async (req, res) => {
    try {
        const dataReferencia = req.query.data || obterHojeBrasilBackend();
        const desenhos = await listarDesenhosCoordenadorRota(req, dataReferencia);

        res.json({
            desenhos,
            permissoes: obterPermissoesDesenhos(req),
            ferramentas: obterFerramentasDesenhoMapa(),
            tiposArea: obterTiposAreaDesenhoMapa(),
            visibilidades: obterVisibilidadesDesenhoMapa()
        });
    } catch (err) {
        responderErroDesenho(res, err, "Não foi possível listar os desenhos do mapa.");
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/desenhos", async (req, res) => {
    if (!validarPermissaoDesenhos(req, res)) return;

    const dataReferencia = req.body?.dataReferencia || req.body?.data_referencia || req.query?.data || obterHojeBrasilBackend();

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const desenho = await criarDesenhoMapa(req.body || {}, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            dataReferencia,
            criadoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("desenho_criar", {
            desenhoId: desenho.id,
            titulo: desenho.titulo,
            ferramenta: desenho.ferramenta,
            tipo: desenho.tipo,
            dataReferencia: desenho.dataReferencia || dataReferencia
        }, req);

        await emitirDesenhosCoordenadorRota(req, desenho.dataReferencia || dataReferencia);
        const desenhos = await listarDesenhosCoordenadorRota(req, desenho.dataReferencia || dataReferencia);
        res.status(201).json({ desenho, desenhos });
    } catch (err) {
        responderErroDesenho(res, err, "Não foi possível criar o desenho do mapa.");
    }
});

app.put("/api/agenda-tecnicos/coordenador-rota/desenhos/:id", async (req, res) => {
    if (!validarPermissaoDesenhos(req, res)) return;

    const dataReferencia = req.body?.dataReferencia || req.body?.data_referencia || req.query?.data || obterHojeBrasilBackend();

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const desenho = await atualizarDesenhoMapa(req.params.id, req.body || {}, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            dataReferencia,
            atualizadoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("desenho_editar", {
            desenhoId: desenho.id,
            titulo: desenho.titulo,
            ferramenta: desenho.ferramenta,
            tipo: desenho.tipo,
            dataReferencia: desenho.dataReferencia || dataReferencia
        }, req);

        await emitirDesenhosCoordenadorRota(req, desenho.dataReferencia || dataReferencia);
        const desenhos = await listarDesenhosCoordenadorRota(req, desenho.dataReferencia || dataReferencia);
        res.json({ desenho, desenhos });
    } catch (err) {
        responderErroDesenho(res, err, "Não foi possível editar o desenho do mapa.");
    }
});

app.delete("/api/agenda-tecnicos/coordenador-rota/desenhos/:id", async (req, res) => {
    if (!validarPermissaoDesenhos(req, res)) return;

    const dataReferencia = req.query?.data || req.body?.dataReferencia || req.body?.data_referencia || obterHojeBrasilBackend();

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const desenho = await removerDesenhoMapa(req.params.id, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            removidoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("desenho_remover", {
            desenhoId: desenho.id,
            titulo: desenho.titulo,
            ferramenta: desenho.ferramenta,
            tipo: desenho.tipo,
            dataReferencia: desenho.dataReferencia || dataReferencia
        }, req);

        await emitirDesenhosCoordenadorRota(req, desenho.dataReferencia || dataReferencia);
        const desenhos = await listarDesenhosCoordenadorRota(req, desenho.dataReferencia || dataReferencia);
        res.json({ desenho, desenhos });
    } catch (err) {
        responderErroDesenho(res, err, "Não foi possível remover o desenho do mapa.");
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/desenhos/:id/salvar-definitivo", async (req, res) => {
    if (!validarPermissaoDesenhos(req, res)) return;

    const dataReferencia = req.body?.dataReferencia || req.body?.data_referencia || req.query?.data || obterHojeBrasilBackend();

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const desenho = await salvarDesenhoDefinitivoMapa(req.params.id, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            atualizadoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("desenho_salvar_definitivo", {
            desenhoId: desenho.id,
            titulo: desenho.titulo,
            ferramenta: desenho.ferramenta
        }, req);

        await emitirDesenhosCoordenadorRota(req, dataReferencia);
        const desenhos = await listarDesenhosCoordenadorRota(req, dataReferencia);
        res.json({ desenho, desenhos });
    } catch (err) {
        responderErroDesenho(res, err, "Não foi possível salvar o desenho como definitivo.");
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/desenhos/limpar-dia", async (req, res) => {
    if (!validarPermissaoDesenhos(req, res)) return;

    const dataReferencia = req.body?.dataReferencia || req.body?.data_referencia || req.query?.data || obterHojeBrasilBackend();

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const removidos = await limparDesenhosDiaMapa(dataReferencia, {
            tecnicosValidos: obterTecnicosValidosCoordenadorRota(filtros),
            removidoPor: obterUsuarioAcaoRota(req)
        });

        registrarLogCoordenadorRota("desenhos_limpar_dia", {
            dataReferencia,
            removidos: removidos.length
        }, req);

        await emitirDesenhosCoordenadorRota(req, dataReferencia);
        const desenhos = await listarDesenhosCoordenadorRota(req, dataReferencia);
        res.json({ removidos: removidos.length, desenhos });
    } catch (err) {
        responderErroDesenho(res, err, "Não foi possível limpar os desenhos do dia.");
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/rastreame/testar", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const dados = await testarFontesRastreame();
        res.json(dados);
    } catch (err) {
        console.error("Erro ao testar RastreaMe:", err.message);
        res.status(500).json({ erro: "Não foi possível testar a integração RastreaMe." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/rastreame/debug-campos", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreame();
        const dados = await debugCamposRastreame({ tecnicos: tecnicosAgendaRastreame });
        res.json(dados);
    } catch (err) {
        console.error("Erro ao inspecionar campos RastreaMe:", err.message);
        res.status(500).json({ erro: "Não foi possível inspecionar os campos da integração RastreaMe." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/rastreame/sincronizar", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreame();
        const dados = await sincronizarRastreameAgora({ force: true, tecnicos: tecnicosAgendaRastreame });
        const dadosFiltrados = filtrarRastreamePorEscopoCoordenadorRota(dados, filtros);
        registrarLogCoordenadorRota("rastreame_sincronizar", {
            totalVeiculos: dadosFiltrados.veiculos?.length || 0,
            fontes: dadosFiltrados.fontes,
            realtimeStatus: dadosFiltrados.realtimeStatus
        }, req);

        res.json(dadosFiltrados);
    } catch (err) {
        console.error("Erro ao sincronizar RastreaMe:", err.message);
        res.status(500).json({ erro: "Não foi possível sincronizar o RastreaMe." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/rastreame/veiculos", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreame();
        await sincronizarPosicoesRastreame({ force: req.query.force === "1", tecnicos: tecnicosAgendaRastreame });
        res.json(filtrarRastreamePorEscopoCoordenadorRota(
            obterVeiculosRastreame({ tecnicos: tecnicosAgendaRastreame }),
            filtros
        ));
    } catch (err) {
        console.error("Erro ao listar veículos RastreaMe:", err.message);
        res.status(500).json({ erro: "Não foi possível listar os veículos RastreaMe." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/rastreame/vincular", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreameCoordenadorRota(filtros);
        const tecnicoSolicitado = req.body?.tecnicoId || req.body?.tecnico_id;

        if (isEscopoInstalacaoCoordenadorRota(filtros) && !tecnicosAgendaRastreame.includes(normalizarTecnicoCoordenadorRota(tecnicoSolicitado))) {
            return res.status(403).json({ erro: "Técnico fora do escopo de instalação para vínculo RastreaMe." });
        }
        const resultado = vincularVeiculoTecnico({
            tecnicoId: tecnicoSolicitado,
            fonte: req.body?.fonte,
            veiculoId: req.body?.veiculoId || req.body?.veiculo_id,
            placa: req.body?.placa,
            nomeVeiculo: req.body?.nomeVeiculo || req.body?.nome_veiculo,
            nomeTecnicoApi: req.body?.nomeTecnicoApi || req.body?.nome_tecnico_api,
            confirmarTroca: Boolean(req.body?.confirmarTroca || req.body?.forcar || req.body?.force)
        });

        registrarLogCoordenadorRota("rastreame_vincular_veiculo", {
            tecnicoId: resultado.vinculo.tecnico_id,
            fonte: resultado.vinculo.fonte,
            veiculoId: resultado.vinculo.veiculo_id,
            placa: resultado.vinculo.placa
        }, req);

        res.json({
            ...resultado,
            rastreame: filtrarRastreamePorEscopoCoordenadorRota(
                obterVeiculosRastreame({ tecnicos: obterListaTecnicosAgendaRastreame() }),
                filtros
            )
        });
    } catch (err) {
        const statusCode = err.statusCode || 500;
        res.status(statusCode).json({
            erro: err.message || "Não foi possível vincular o veículo.",
            conflito: err.conflito || null
        });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/rastreame/desvincular", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreameCoordenadorRota(filtros);
        const tecnicoSolicitado = req.body?.tecnicoId || req.body?.tecnico_id;

        if (isEscopoInstalacaoCoordenadorRota(filtros) && !tecnicosAgendaRastreame.includes(normalizarTecnicoCoordenadorRota(tecnicoSolicitado))) {
            return res.status(403).json({ erro: "Técnico fora do escopo de instalação para desvincular RastreaMe." });
        }
        const resultado = desvincularVeiculoTecnico({
            tecnicoId: tecnicoSolicitado,
            fonte: req.body?.fonte,
            veiculoId: req.body?.veiculoId || req.body?.veiculo_id,
            placa: req.body?.placa
        });

        registrarLogCoordenadorRota("rastreame_desvincular_veiculo", {
            tecnicoId: tecnicoSolicitado,
            alterados: resultado.alterados
        }, req);

        res.json({
            ...resultado,
            rastreame: filtrarRastreamePorEscopoCoordenadorRota(
                obterVeiculosRastreame({ tecnicos: obterListaTecnicosAgendaRastreame() }),
                filtros
            )
        });
    } catch (err) {
        console.error("Erro ao desvincular veículo RastreaMe:", err.message);
        res.status(500).json({ erro: "Não foi possível desvincular o veículo." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/posicoes-atualizadas", async (req, res) => {
    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreame();
        sincronizarRastreameSeNecessario({ tecnicos: tecnicosAgendaRastreame });
        res.json(filtrarRastreamePorEscopoCoordenadorRota(
            obterRastreameRealtimeSnapshot({ tecnicos: tecnicosAgendaRastreame }),
            filtros
        ));
    } catch (err) {
        console.error("Erro ao obter posições atualizadas:", err.message);
        res.status(500).json({ erro: "Não foi possível carregar as posições atualizadas." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/posicoes-tempo-real", async (req, res) => {
    try {
        const filtros = obterFiltrosRequestCoordenadorRota(req);
        const tecnicosAgendaRastreame = obterListaTecnicosAgendaRastreame();
        sincronizarRastreameSeNecessario({ tecnicos: tecnicosAgendaRastreame });
        res.json(filtrarRastreamePorEscopoCoordenadorRota(
            obterRastreameRealtimeSnapshot({ tecnicos: tecnicosAgendaRastreame }),
            filtros
        ));
    } catch (err) {
        console.error("Erro ao obter posições em tempo real:", err.message);
        res.status(500).json({ erro: "Não foi possível carregar as posições em tempo real." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/rastreame/realtime-status", (req, res) => {
    res.json(getRastreameRealtimeStatus());
});

function isRequestLocal(req) {
    const enderecos = [req.ip, req.socket?.remoteAddress].filter(Boolean);
    return enderecos.some(endereco => (
        endereco === "127.0.0.1" ||
        endereco === "::1" ||
        endereco === "::ffff:127.0.0.1"
    ));
}

app.post("/api/agenda-tecnicos/forcar-reload-clientes", (req, res) => {
    if (!isRequestLocal(req)) {
        return res.status(403).json({ erro: "Comando disponível apenas localmente no servidor." });
    }

    const delayMs = Math.max(0, Math.min(Number(req.body?.delayMs) || 300, 5000));
    const payload = {
        delayMs,
        motivo: req.body?.motivo || "atualizacao",
        criadoEm: new Date().toISOString()
    };

    io.emit("agendaTecnicos:forcarReload", payload);
    res.json({
        ok: true,
        clientes: io.engine.clientsCount,
        ...payload
    });
});

app.post("/api/agenda-tecnicos/coordenador-rota/sugerir", async (req, res) => {
    try {
        const filtros = normalizarFiltrosCoordenadorRota({
            ...req.query,
            ...req.body
        });
        const dados = await montarDadosCoordenadorRota(filtros, req);
        const sugestoesGeradas = gerarSugestoesCoordenadorRota({
            data: filtros.data,
            tecnicos: dados.tecnicos,
            chamados: dados.chamados,
            rotas: dados.rotas
        });
        const sugestoes = filtrarSugestoesCoordenadorRota(salvarSugestoesGeradasRota(sugestoesGeradas, req), filtros);

        res.json({
            ...dados,
            sugestoes,
            geradas: sugestoesGeradas.length
        });
    } catch (err) {
        console.error("Erro ao gerar sugestões do coordenador de rota:", err.message);
        res.status(500).json({ erro: "Não foi possível gerar sugestões de rota." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/localizacao/reprocessar", async (req, res) => {
    try {
        const dados = await reprocessarLocalizacoesCoordenadorRota({
            ...req.query,
            ...req.body
        }, req);

        res.json(dados);
    } catch (err) {
        console.error("Erro ao reprocessar localizações do coordenador de rota:", err.message);
        res.status(500).json({ erro: "Não foi possível reprocessar as localizações." });
    }
});

app.get("/api/agenda-tecnicos/coordenador-rota/debug-chamado", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const debug = await montarDebugChamadoCoordenadorRota(req.query, req.query);
        res.json(debug);
    } catch (err) {
        console.error("Erro ao depurar chamado do coordenador de rota:", err.message);
        res.status(err.statusCode || 500).json({ erro: err.message || "Não foi possível depurar o chamado." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/sgp/sincronizar-localizacoes", async (req, res) => {
    try {
        const resultado = await sincronizarLocalizacoesSgpCoordenadorRota({
            ...req.query,
            ...req.body
        }, req, null, {
            substituirManual: false
        });

        res.json(resultado);
    } catch (err) {
        console.error("Erro ao sincronizar localizações SGP do coordenador de rota:", err.message);
        res.status(err.statusCode || 500).json({ erro: err.message || "Não foi possível sincronizar as localizações do SGP." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/sgp/sincronizar-localizacao-cliente", async (req, res) => {
    if (!validarPermissaoAdminCoordenadorRota(req, res)) return;

    try {
        const resultado = await sincronizarLocalizacaoClienteSgpCoordenadorRota({
            ...req.query,
            ...req.body
        }, {
            ...req.query,
            ...req.body
        }, req);

        res.json(resultado);
    } catch (err) {
        console.error("Erro ao sincronizar localização SGP por cliente:", err.message);
        res.status(err.statusCode || 500).json({ erro: err.message || "Não foi possível sincronizar a localização do cliente no SGP." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/sgp/sincronizar-localizacao/:chamadoId", async (req, res) => {
    try {
        const resultado = await sincronizarLocalizacoesSgpCoordenadorRota({
            ...req.query,
            ...req.body
        }, req, req.params.chamadoId, {
            substituirManual: req.body?.substituirManual === true || req.body?.substituirManual === "true"
        });

        res.json(resultado);
    } catch (err) {
        console.error("Erro ao sincronizar localização SGP do chamado:", err.message);
        res.status(err.statusCode || 500).json({ erro: err.message || "Não foi possível sincronizar a localização do SGP." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/aplicar", async (req, res) => {
    try {
        const sugestaoId = Number(req.body?.sugestaoId || req.body?.id);
        const store = lerStoreSugestoesRota();
        const sugestao = store.items.find(item => item.id === sugestaoId);

        if (!sugestao) {
            return res.status(404).json({ erro: "Sugestão não encontrada." });
        }

        if (sugestao.status === "rejeitada") {
            return res.status(409).json({ erro: "Sugestão rejeitada não pode ser aplicada." });
        }

        const agora = new Date().toISOString();
        sugestao.status = "aplicada";
        sugestao.applied_at = agora;
        sugestao.rejected_at = null;
        sugestao.updated_at = agora;
        sugestao.usuario_acao = obterUsuarioAcaoRota(req);

        salvarStoreSugestoesRota(store);
        registrarLogCoordenadorRota("aplicar_sugestao", {
            sugestaoId: sugestao.id,
            tipo_sugestao: sugestao.tipo_sugestao,
            chamado_id: sugestao.chamado_id,
            tecnico_atual_id: sugestao.tecnico_atual_id,
            tecnico_sugerido_id: sugestao.tecnico_sugerido_id
        }, req);

        const dados = await montarDadosCoordenadorRota({
            data: sugestao.data_sugestao,
            tecnicoId: req.body?.tecnicoId,
            regiao: req.body?.regiao,
            status: req.body?.status,
            motivo: req.body?.motivo
        }, req);

        res.json({
            sugestao,
            ...dados
        });
    } catch (err) {
        console.error("Erro ao aplicar sugestão do coordenador de rota:", err.message);
        res.status(500).json({ erro: "Não foi possível aplicar a sugestão." });
    }
});

app.post("/api/agenda-tecnicos/coordenador-rota/rejeitar", async (req, res) => {
    try {
        const sugestaoId = Number(req.body?.sugestaoId || req.body?.id);
        const store = lerStoreSugestoesRota();
        const sugestao = store.items.find(item => item.id === sugestaoId);

        if (!sugestao) {
            return res.status(404).json({ erro: "Sugestão não encontrada." });
        }

        if (sugestao.status === "aplicada") {
            return res.status(409).json({ erro: "Sugestão aplicada não pode ser rejeitada." });
        }

        const agora = new Date().toISOString();
        sugestao.status = "rejeitada";
        sugestao.rejected_at = agora;
        sugestao.applied_at = null;
        sugestao.updated_at = agora;
        sugestao.usuario_acao = obterUsuarioAcaoRota(req);

        salvarStoreSugestoesRota(store);
        registrarLogCoordenadorRota("rejeitar_sugestao", {
            sugestaoId: sugestao.id,
            tipo_sugestao: sugestao.tipo_sugestao,
            chamado_id: sugestao.chamado_id
        }, req);

        const dados = await montarDadosCoordenadorRota({
            data: sugestao.data_sugestao,
            tecnicoId: req.body?.tecnicoId,
            regiao: req.body?.regiao,
            status: req.body?.status,
            motivo: req.body?.motivo
        }, req);

        res.json({
            sugestao,
            ...dados
        });
    } catch (err) {
        console.error("Erro ao rejeitar sugestão do coordenador de rota:", err.message);
        res.status(500).json({ erro: "Não foi possível rejeitar a sugestão." });
    }
});


function criarAssinaturaAgendaSocket(agenda = []) {
    const resumo = (Array.isArray(agenda) ? agenda : []).map(os => [
        os.id,
        os.idOrdemServico,
        os.idCliente,
        os.tecnico,
        os.cliente,
        os.hora,
        os.status,
        os.Checkin,
        os.data_finalizado,
        os.bairro,
        os.pop,
        os.motivo,
        os.responsavelStatus
    ]);

    return crypto
        .createHash("sha1")
        .update(JSON.stringify(resumo))
        .digest("hex");
}

let almocos = {};

io.on("connection", (socket) => {
    console.log("🟢 Cliente conectado");

    socket.on("filtrarData", async (dataSelecionada) => {
        if (!dataSelecionada) return;

        const requestId = (socket.data.agendaRequestId || 0) + 1;
        const dataAnterior = socket.data.dataSelecionada;
        socket.data.agendaRequestId = requestId;
        socket.data.dataSelecionada = dataSelecionada;

        if (dataAnterior !== dataSelecionada) {
            socket.data.agendaAssinaturaAtual = "";
        }

        const isRequestAtual = () => (
            socket.connected &&
            socket.data.agendaRequestId === requestId &&
            socket.data.dataSelecionada === dataSelecionada
        );

        const emitirAgenda = async (ordens, evento = "agenda") => {
            if (!isRequestAtual()) return null;

            const ordensAgenda = filtrarOrdensAgendaTecnica(ordens);
            const contratosPendentes = obterContratosPendentes(ordensAgenda);
            const agenda = filtrarAgendaMontadaAgendaTecnica(
                await montarAgenda(ordensAgenda, new Map(), { dataFiltro: dataSelecionada })
            );
            const assinatura = criarAssinaturaAgendaSocket(agenda);
            const alterou = assinatura !== socket.data.agendaAssinaturaAtual;

            if (alterou) {
                socket.data.agendaAssinaturaAtual = assinatura;
                socket.emit(evento, agenda);
            } else {
                socket.emit("agendaSemAlteracao", { data: dataSelecionada });
            }

            return {
                agenda,
                ordensAgenda,
                contratosPendentes,
                alterou
            };
        };

        const emitirAgendaEnriquecida = (ordensAgenda, contratosPendentes) => {
            if (!contratosPendentes.length) return;

            enriquecerAgendaComContratosCompartilhado(ordensAgenda, contratosPendentes, { dataFiltro: dataSelecionada })
                .then(async (agendaEnriquecida) => {
                    if (!agendaEnriquecida) return;
                    if (!isRequestAtual()) return;

                    const agendaFiltrada = filtrarAgendaMontadaAgendaTecnica(agendaEnriquecida);
                    const assinatura = criarAssinaturaAgendaSocket(agendaFiltrada);

                    if (assinatura === socket.data.agendaAssinaturaAtual) {
                        socket.emit("agendaSemAlteracao", { data: dataSelecionada });
                        return;
                    }

                    socket.data.agendaAssinaturaAtual = assinatura;
                    socket.emit("agendaAtualizada", agendaFiltrada);
                })
                .catch((err) => {
                    console.error("⚠️ Erro ao atualizar bairros da agenda:", err.message);
                });
        };

        const cached = agendaCache.get(dataSelecionada);
        const cacheDentroJanelaStale = Boolean(
            cached?.data?.length &&
            cached.expiresAt + AGENDA_STALE_CACHE_TTL_MS > Date.now()
        );
        const cachePossuiAgendaTecnica = cacheDentroJanelaStale && filtrarOrdensAgendaTecnica(cached.data).length > 0;
        const ultimaValida = obterUltimaAgendaValida(dataSelecionada);
        const ordensImediatas = cachePossuiAgendaTecnica ? cached.data : ultimaValida?.data;
        const cacheFresco = Boolean(cachePossuiAgendaTecnica && cached.expiresAt > Date.now());
        let resultadoImediato = null;

        if (ordensImediatas?.length) {
            resultadoImediato = await emitirAgenda(ordensImediatas, "agenda");

            if (cacheFresco) {
                if (resultadoImediato) {
                    emitirAgendaEnriquecida(resultadoImediato.ordensAgenda, resultadoImediato.contratosPendentes);
                }
                return;
            }
        }

        const inicio = Date.now();

        try {
            console.log(`🔄 Atualizando agenda ${dataSelecionada} no SGP${ordensImediatas?.length ? " em segundo plano" : ""}.`);

            const todasOS = await buscarOrdensServico(dataSelecionada);
            if (!isRequestAtual()) return;

            const ordensComTecnico = filtrarOrdensAgendaTecnica(todasOS);
            const ordensParaEmitir = ordensComTecnico.length
                ? todasOS
                : (ultimaValida?.data || ordensImediatas || todasOS);

            const resultadoFinal = await emitirAgenda(ordensParaEmitir, resultadoImediato ? "agendaAtualizada" : "agenda");
            console.log(`✅ Agenda base de ${dataSelecionada} processada em ${Date.now() - inicio}ms`);

            if (resultadoFinal) {
                emitirAgendaEnriquecida(resultadoFinal.ordensAgenda, resultadoFinal.contratosPendentes);
            }
        } catch (err) {
            console.error("⚠️ Erro ao atualizar agenda no SGP:", err.message);
            if (isRequestAtual()) {
                socket.emit("agendaErro", {
                    data: dataSelecionada,
                    erro: "Não foi possível atualizar a agenda no SGP."
                });
            }
        }
    });

    socket.emit("almocosUpdate", almocos);
    socket.emit("plantao_atualizado", plantao);
    socket.emit("rastreame:posicoes_atualizadas", obterRastreameRealtimeSnapshot({
        tecnicos: obterListaTecnicosAgendaRastreame()
    }));

    socket.on("toggleAlmoco", ({ tecnico, inicio, acao }) => {

        if (acao === "parar") {
            delete almocos[tecnico];
        }

        if (acao === "iniciar") {
            almocos[tecnico] = {
                inicio: inicio || Date.now()
            };
        }

        io.emit("almocosUpdate", almocos);
    });


    socket.on("addPlantao", ({ area, nome }) => {
        if (!AREAS.includes(area)) return;
        if (!nome) return;

        // evita duplicado
        if (!plantao[area].includes(nome)) {
            plantao[area].push(nome);
        }

        io.emit("plantao_atualizado", plantao);
    });

    // =============================
    // 🔥 REMOVER (POR NOME)
    // =============================
    socket.on("removerPlantao", ({ area, nome, index }) => {
        if (!AREAS.includes(area)) return;

        if (nome) {
            plantao[area] = plantao[area].filter(n => n !== nome);
        } else if (Number.isInteger(index)) {
            plantao[area].splice(index, 1);
        }

        io.emit("plantao_atualizado", plantao);
    });



    socket.on("disconnect", () => {
        console.log("🔴 Cliente desconectado");
    });
});


app.get(["/", "/agenda-tecnicos.html"], (_req, res) => {
    res.sendFile(INDEX_HTML_FILE);
});

app.use(express.static(PUBLIC_DIR));

server.listen(PORT, "0.0.0.0", () => {
    console.log("🚀 Servidor rodando na porta:", PORT);
    iniciarRastreameRealtime({
        io,
        tecnicos: obterListaTecnicosAgendaRastreame()
    });
});

["SIGINT", "SIGTERM"].forEach(signal => {
    process.once(signal, () => {
        pararRastreameRealtime();
        process.exit(0);
    });
});
