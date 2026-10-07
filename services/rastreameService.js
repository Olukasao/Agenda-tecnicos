import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import fetch from "node-fetch";
import dotenv from "dotenv";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.join(__dirname, "..", "data");
dotenv.config({ path: path.join(__dirname, "..", ".env") });

const RASTREAME_VINCULOS_FILE = path.join(DATA_DIR, "rastreame_veiculos_tecnicos.json");
const RASTREAME_POSICOES_FILE = path.join(DATA_DIR, "rastreame_posicoes.json");
const RASTREAME_POSICOES_ATUAIS_FILE = path.join(DATA_DIR, "rastreame_posicoes_atuais.json");
const RASTREAME_API_URL_PADRAO = "https://api.appselsyn.com.br/keek/rest/v1/integracao/posicao";
const RASTREAME_TIMEOUT_MS = Number(process.env.RASTREAME_TIMEOUT_MS) || 12000;
export const RASTREAME_CACHE_TTL_MS = 60 * 1000;

export const RASTREAME_FONTES = [
    { nome: "ACESSANET", envKey: "RASTREAME_APIKEY_ACESSANET" },
    { nome: "CDE", envKey: "RASTREAME_APIKEY_CDE" },
    { nome: "DEC", envKey: "RASTREAME_APIKEY_DEC" }
];

const ALIASES_RASTREAME = {
    latitude: ["latitude", "lat", "Latitude", "LAT"],
    longitude: ["longitude", "lng", "lon", "Longitude", "LON"],
    placa: ["placa", "plate", "Placa", "identificador"],
    veiculoId: ["veiculo", "veiculoId", "idVeiculo", "equipamento", "dispositivo", "id", "codigo", "codVeiculo", "identificador"],
    nomeVeiculo: ["nomeVeiculo", "nome_veiculo", "rastreavel", "veiculo", "nome", "descricao", "apelido", "equipamento"],
    dataPosicao: ["data", "dataHora", "dh", "dataPosicao", "ultimaAtualizacao", "data_posicao", "dt"],
    velocidade: ["velocidade", "speed"],
    ignicao: ["ignicao", "ignition", "ligado"],
    endereco: ["endereco", "address", "local", "logradouro", "posicao"]
};

const CAMPOS_NOME_TECNICO_RASTREAME = [
    "tecnico",
    "nomeTecnico",
    "motorista",
    "nomeMotorista",
    "motoristaNome",
    "condutor",
    "nomeCondutor",
    "driver",
    "driverName",
    "usuario",
    "responsavel",
    "operador",
    "motoristaIdentificado",
    "funcao",
    "função"
];

const CAMPOS_POSSIVEIS_MOTORISTA_RASTREAME = [
    ...CAMPOS_NOME_TECNICO_RASTREAME,
    "nome",
    "idMotorista",
    "idCondutor",
    "tagMotorista",
    "rfid",
    "ibutton"
];
const VINCULO_ORIGENS = new Set(["manual", "api_nome_tecnico", "placa", "sem_vinculo"]);
const CONFIANCAS_VINCULO = new Set(["alta", "media", "baixa"]);
const NORMALIZAR_CAMPO_RASTREAME_CACHE_MAX = 2000;
const normalizarCampoRastreameCache = new Map();

let lastSyncRastreameAt = 0;
let isSyncingRastreame = false;
let syncPromise = null;
let lastSyncResult = null;
let lastSyncRastreameError = null;
let tecnicosAgendaRastreame = [];

function ensureDataDir() {
    if (!fs.existsSync(DATA_DIR)) {
        fs.mkdirSync(DATA_DIR, { recursive: true });
    }
}

function lerJson(file, fallback) {
    try {
        if (!fs.existsSync(file)) return fallback;
        return JSON.parse(fs.readFileSync(file, "utf8"));
    } catch (err) {
        console.error(`Erro ao ler ${path.basename(file)}:`, err.message);
        return fallback;
    }
}

function salvarJson(file, data) {
    ensureDataDir();
    fs.writeFileSync(file, JSON.stringify(data, null, 2));
}

function normalizarTexto(valor) {
    return String(valor || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .trim()
        .toLowerCase();
}

export function normalizarNome(nome) {
    return String(nome || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .replace(/[^a-zA-Z0-9\s]/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .toLowerCase();
}

function normalizarListaTecnicos(tecnicos = []) {
    return [...new Set((Array.isArray(tecnicos) ? tecnicos : [])
        .map(tecnico => String(tecnico || "").trim())
        .filter(Boolean))];
}

export function configurarTecnicosAgendaRastreame(tecnicos = []) {
    tecnicosAgendaRastreame = normalizarListaTecnicos(tecnicos);
    return tecnicosAgendaRastreame;
}

function obterTecnicosAgendaRastreame(tecnicos = null) {
    const lista = tecnicos ? normalizarListaTecnicos(tecnicos) : tecnicosAgendaRastreame;
    return normalizarListaTecnicos(lista);
}

function valorCampo(obj, aliases) {
    if (!obj || typeof obj !== "object") return undefined;

    for (const alias of aliases) {
        if (Object.prototype.hasOwnProperty.call(obj, alias)) return obj[alias];
    }

    const entradas = Object.entries(obj);
    for (const alias of aliases) {
        const aliasNormalizado = normalizarTexto(alias);
        const encontrado = entradas.find(([chave]) => normalizarTexto(chave) === aliasNormalizado);
        if (encontrado) return encontrado[1];
    }

    return undefined;
}

function valorCampoProfundo(obj, aliases, profundidade = 0) {
    if (!obj || typeof obj !== "object" || profundidade > 3) return undefined;

    const direto = valorCampo(obj, aliases);
    if (direto !== undefined && direto !== null && direto !== "") return direto;

    const valores = Array.isArray(obj) ? obj : Object.values(obj);
    for (const valor of valores) {
        if (!valor || typeof valor !== "object") continue;
        const encontrado = valorCampoProfundo(valor, aliases, profundidade + 1);
        if (encontrado !== undefined && encontrado !== null && encontrado !== "") return encontrado;
    }

    return undefined;
}

function limparNomeTecnicoExtraido(valor) {
    if (valor === null || valor === undefined) return "";
    if (Array.isArray(valor)) {
        return valor.map(limparNomeTecnicoExtraido).find(Boolean) || "";
    }
    if (typeof valor === "object") {
        return limparNomeTecnicoExtraido(valorCampoProfundo(valor, CAMPOS_NOME_TECNICO_RASTREAME));
    }
    if (typeof valor === "number" || typeof valor === "boolean") return "";
    return String(valor).replace(/\s+/g, " ").trim();
}

function normalizarCampoRastreame(campo) {
    const chave = String(campo || "");
    const cached = normalizarCampoRastreameCache.get(chave);

    if (cached !== undefined) return cached;

    const normalizado = chave
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toLowerCase()
        .replace(/[^a-z0-9]/g, "");

    if (normalizarCampoRastreameCache.size >= NORMALIZAR_CAMPO_RASTREAME_CACHE_MAX) {
        normalizarCampoRastreameCache.clear();
    }

    normalizarCampoRastreameCache.set(chave, normalizado);
    return normalizado;
}

const CAMPOS_POSSIVEIS_MOTORISTA_NORMALIZADOS = CAMPOS_POSSIVEIS_MOTORISTA_RASTREAME.map(normalizarCampoRastreame);
const CAMPOS_IDENTIFICADORES_MOTORISTA_NORMALIZADOS = ["idmotorista", "idcondutor", "tagmotorista", "rfid", "ibutton"]
    .map(normalizarCampoRastreame);
const CONTEXTOS_NOME_MOTORISTA_NORMALIZADOS = ["motorista", "condutor", "tecnico", "driver", "usuario", "responsavel", "operador"]
    .map(normalizarCampoRastreame);

function listarCamposProfundos(item, caminho = "", profundidade = 0) {
    if (!item || typeof item !== "object" || profundidade > 4) return [];

    const entradas = Array.isArray(item)
        ? item.slice(0, 3).map((valor, index) => [String(index), valor])
        : Object.entries(item);

    return entradas.flatMap(([chave, valor]) => {
        const caminhoAtual = caminho ? `${caminho}.${chave}` : chave;
        const atual = [{
            caminho: caminhoAtual,
            chave,
            chaveNormalizada: normalizarCampoRastreame(chave),
            valor
        }];

        if (valor && typeof valor === "object") {
            return atual.concat(listarCamposProfundos(valor, caminhoAtual, profundidade + 1));
        }

        return atual;
    });
}

function isCampoMotoristaRastreame(campo) {
    const chave = normalizarCampoRastreame(campo.chave);
    const caminho = normalizarCampoRastreame(campo.caminho);
    return CAMPOS_POSSIVEIS_MOTORISTA_NORMALIZADOS.some(aliasNormalizado => {
        return chave === aliasNormalizado || caminho.endsWith(aliasNormalizado) || caminho.includes(aliasNormalizado);
    });
}

function isCampoIdentificadorMotorista(campo) {
    const caminho = normalizarCampoRastreame(campo.caminho);
    return CAMPOS_IDENTIFICADORES_MOTORISTA_NORMALIZADOS.some(alias => caminho.includes(alias));
}

function isCampoNomeGenericoSemContexto(campo) {
    if (normalizarCampoRastreame(campo.chave) !== "nome") return false;
    const caminho = normalizarCampoRastreame(campo.caminho);
    return !CONTEXTOS_NOME_MOTORISTA_NORMALIZADOS.some(contexto => caminho.includes(contexto));
}

function obterCamposPossiveisMotorista(item) {
    return listarCamposProfundos(item)
        .filter(isCampoMotoristaRastreame)
        .filter(campo => campo.valor !== undefined && campo.valor !== null && campo.valor !== "");
}

function selecionarNomeMotoristaApi(item) {
    const candidatos = obterCamposPossiveisMotorista(item)
        .filter(campo => !isCampoIdentificadorMotorista(campo))
        .filter(campo => !isCampoNomeGenericoSemContexto(campo))
        .map(campo => ({
            ...campo,
            texto: limparNomeTecnicoExtraido(campo.valor)
        }))
        .filter(campo => campo.texto && /[a-zA-ZÀ-ÿ]/.test(campo.texto));

    return candidatos[0] || null;
}

function camposDisponiveisProfundos(item) {
    return listarCamposProfundos(item)
        .map(campo => campo.caminho)
        .sort((a, b) => a.localeCompare(b, "pt-BR"));
}

function montarCamposPossiveisMotorista(item) {
    return Object.fromEntries(obterCamposPossiveisMotorista(item)
        .map(campo => [campo.caminho, sanitizarValor(campo.valor)]));
}

export function extrairNomeTecnicoRastreame(item) {
    return selecionarNomeMotoristaApi(item)?.texto || "";
}

function montarIndiceTecnicosPorNome(tecnicos = null) {
    return obterTecnicosAgendaRastreame(tecnicos)
        .map(nome => {
            const nomeNormalizado = normalizarNome(nome);
            const primeiroNome = nomeNormalizado.split(" ")[0] || "";
            return {
                id: nome,
                nome,
                nomeNormalizado,
                primeiroNome
            };
        })
        .filter(tecnico => tecnico.nomeNormalizado);
}

export function encontrarTecnicoPorNome(nomeRastreame, tecnicos = null) {
    const nomeNormalizado = normalizarNome(nomeRastreame);
    if (!nomeNormalizado) return null;

    const indice = montarIndiceTecnicosPorNome(tecnicos);
    if (!indice.length) return null;

    const tokens = nomeNormalizado.split(" ").filter(Boolean);
    const primeiroNome = tokens[0] || "";
    const exatos = indice.filter(tecnico => tecnico.nomeNormalizado === nomeNormalizado);

    if (exatos.length === 1) {
        if (tokens.length === 1) {
            const ambiguos = indice.filter(tecnico => {
                if (tecnico.id === exatos[0].id) return false;
                if (tecnico.primeiroNome === primeiroNome) return true;
                return tecnico.primeiroNome.startsWith(primeiroNome) || primeiroNome.startsWith(tecnico.primeiroNome);
            });
            if (ambiguos.length) return null;
        }

        return {
            tecnicoId: exatos[0].id,
            tecnicoNome: exatos[0].nome,
            confiancaVinculo: "alta",
            tipoMatch: "exato"
        };
    }

    if (exatos.length > 1 || !primeiroNome) return null;

    const riscoPrefixo = indice.filter(tecnico => (
        tecnico.primeiroNome
        && tecnico.primeiroNome !== primeiroNome
        && (tecnico.primeiroNome.startsWith(primeiroNome) || primeiroNome.startsWith(tecnico.primeiroNome))
    ));
    if (riscoPrefixo.length) return null;

    const candidatosPrimeiroNome = indice.filter(tecnico => tecnico.primeiroNome === primeiroNome);
    if (candidatosPrimeiroNome.length !== 1) return null;

    return {
        tecnicoId: candidatosPrimeiroNome[0].id,
        tecnicoNome: candidatosPrimeiroNome[0].nome,
        confiancaVinculo: "media",
        tipoMatch: "primeiro_nome"
    };
}

function parseNumero(valor) {
    if (valor === null || valor === undefined || valor === "") return null;
    const numero = Number(String(valor).replace(",", "."));
    return Number.isFinite(numero) ? numero : null;
}

function validarCoordenada(latitude, longitude) {
    const lat = parseNumero(latitude);
    const lng = parseNumero(longitude);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
    if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
    if (lat === 0 && lng === 0) return null;

    return { latitude: lat, longitude: lng };
}

function parseBoolean(valor) {
    if (typeof valor === "boolean") return valor;
    if (typeof valor === "number") return valor > 0;
    const texto = normalizarTexto(valor);
    if (!texto) return null;
    if (["true", "1", "sim", "s", "ligado", "ligada", "on"].includes(texto)) return true;
    if (["false", "0", "nao", "n", "desligado", "desligada", "off"].includes(texto)) return false;
    return null;
}

function parseDataIso(valor) {
    if (valor === null || valor === undefined || valor === "") return null;

    if (typeof valor === "number") {
        const ms = valor < 10000000000 ? valor * 1000 : valor;
        const data = new Date(ms);
        return Number.isNaN(data.getTime()) ? null : data.toISOString();
    }

    const texto = String(valor).trim();
    const br = texto.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T]+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
    if (br) {
        const [, dia, mes, ano, hora = "0", minuto = "0", segundo = "0"] = br;
        const data = new Date(Number(ano), Number(mes) - 1, Number(dia), Number(hora), Number(minuto), Number(segundo));
        return Number.isNaN(data.getTime()) ? null : data.toISOString();
    }

    const data = new Date(texto);
    return Number.isNaN(data.getTime()) ? texto : data.toISOString();
}

function sanitizarValor(valor, profundidade = 0) {
    if (profundidade > 3) return "[objeto]";
    if (Array.isArray(valor)) return valor.slice(0, 3).map(item => sanitizarValor(item, profundidade + 1));
    if (!valor || typeof valor !== "object") return valor;

    return Object.fromEntries(Object.entries(valor).map(([chave, item]) => {
        const chaveNormalizada = normalizarTexto(chave);
        if (/(api|key|token|senha|password|authorization|auth)/.test(chaveNormalizada)) {
            return [chave, "[redacted]"];
        }
        return [chave, sanitizarValor(item, profundidade + 1)];
    }));
}

function extrairItensResposta(payload) {
    if (Array.isArray(payload)) return payload;
    if (!payload || typeof payload !== "object") return [];

    const chavesPreferidas = [
        "veiculos",
        "vehicles",
        "posicoes",
        "positions",
        "items",
        "dados",
        "data",
        "result",
        "results",
        "retorno"
    ];

    for (const chave of chavesPreferidas) {
        if (Array.isArray(payload[chave])) return payload[chave];
        if (payload[chave] && typeof payload[chave] === "object") {
            const encontrados = extrairItensResposta(payload[chave]);
            if (encontrados.length) return encontrados;
        }
    }

    const primeiraLista = Object.values(payload).find(valor => Array.isArray(valor));
    if (primeiraLista) return primeiraLista;

    return [];
}

function camposDisponiveis(item) {
    if (!item || typeof item !== "object") return [];
    return Object.keys(item).sort((a, b) => a.localeCompare(b, "pt-BR"));
}

function isApiKeyConfigurada(apiKey) {
    const valor = String(apiKey || "").trim();
    if (!valor) return false;
    if (valor.includes("colocar_chave")) return false;
    if (valor.includes("aqui")) return false;
    return true;
}

function obterApiKeyFonte(fonte) {
    return process.env[fonte.envKey];
}

function normalizarFonte(fonte) {
    const texto = normalizarTexto(fonte).toUpperCase();
    return RASTREAME_FONTES.find(item => item.nome === texto)?.nome || "";
}

function normalizarVinculoOrigem(origem, fallback = "sem_vinculo") {
    const texto = normalizarNome(origem).replace(/\s+/g, "_");
    return VINCULO_ORIGENS.has(texto) ? texto : fallback;
}

function normalizarConfiancaVinculo(confianca, fallback = null) {
    const texto = normalizarNome(confianca);
    return CONFIANCAS_VINCULO.has(texto) ? texto : fallback;
}

function montarUrlRastreame(apiKey) {
    const url = new URL(process.env.RASTREAME_API_URL || RASTREAME_API_URL_PADRAO);
    url.searchParams.set("apikey", apiKey);
    return url;
}

function chaveVeiculo({ fonte, veiculoId, placa }) {
    return `${String(fonte || "").toUpperCase()}|${String(veiculoId || "").trim() || normalizarTexto(placa)}`;
}

function mesmoVeiculo(a, b) {
    if (String(a?.fonte || "").toUpperCase() !== String(b?.fonte || "").toUpperCase()) return false;
    const aId = String(a?.veiculo_id || a?.veiculoId || "").trim();
    const bId = String(b?.veiculo_id || b?.veiculoId || "").trim();
    const aPlaca = normalizarTexto(a?.placa);
    const bPlaca = normalizarTexto(b?.placa);
    return Boolean((aId && bId && aId === bId) || (aPlaca && bPlaca && aPlaca === bPlaca));
}

export function normalizarPosicaoRastreame(item, fonte, index = 0) {
    const coordenada = validarCoordenada(
        valorCampo(item, ALIASES_RASTREAME.latitude),
        valorCampo(item, ALIASES_RASTREAME.longitude)
    );
    const placa = String(valorCampo(item, ALIASES_RASTREAME.placa) || "").trim().toUpperCase();
    const nomeVeiculo = String(valorCampo(item, ALIASES_RASTREAME.nomeVeiculo) || placa || "").trim();
    const veiculoId = String(valorCampo(item, ALIASES_RASTREAME.veiculoId) || placa || nomeVeiculo || `${fonte}-${index + 1}`).trim();
    const velocidade = parseNumero(valorCampo(item, ALIASES_RASTREAME.velocidade));
    const ignicao = parseBoolean(valorCampo(item, ALIASES_RASTREAME.ignicao));
    const dataPosicao = parseDataIso(valorCampo(item, ALIASES_RASTREAME.dataPosicao));
    const endereco = String(valorCampo(item, ALIASES_RASTREAME.endereco) || "").trim();
    const nomeTecnicoRastreame = extrairNomeTecnicoRastreame(item);

    return {
        fonte,
        veiculoId,
        placa,
        nomeVeiculo,
        nomeTecnicoRastreame,
        nomeTecnicoApi: nomeTecnicoRastreame,
        nomeMotoristaApi: nomeTecnicoRastreame,
        latitude: coordenada?.latitude ?? null,
        longitude: coordenada?.longitude ?? null,
        velocidade,
        ignicao,
        dataPosicao,
        endereco,
        raw: sanitizarValor(item || {})
    };
}

export async function debugCamposRastreame({ tecnicos = null } = {}) {
    const resultado = await buscarTodasPosicoesRastreame();

    return {
        fontes: resultado.fontes.map(fonte => {
            const primeiroItem = fonte.primeiroItemSanitizado || fonte.veiculos?.[0]?.raw || {};
            const nomeMotorista = extrairNomeTecnicoRastreame(primeiroItem);
            const matchTecnico = encontrarTecnicoPorNome(nomeMotorista, tecnicos);

            return {
                fonte: fonte.fonte,
                sucesso: fonte.sucesso,
                totalVeiculos: fonte.total || 0,
                camposDisponiveis: camposDisponiveisProfundos(primeiroItem),
                camposPrimeiroNivel: fonte.camposDetectados || camposDisponiveis(primeiroItem),
                camposPossiveisMotorista: montarCamposPossiveisMotorista(primeiroItem),
                nomeTecnicoApi: nomeMotorista || null,
                nomeMotoristaApi: nomeMotorista || null,
                tecnicoAgendaDetectado: matchTecnico ? {
                    tecnicoId: matchTecnico.tecnicoId,
                    tecnicoNome: matchTecnico.tecnicoNome,
                    confiancaVinculo: matchTecnico.confiancaVinculo,
                    tipoMatch: matchTecnico.tipoMatch
                } : null,
                primeiroItemSanitizado: primeiroItem,
                erro: fonte.erro || null
            };
        })
    };
}

export async function buscarPosicoesPorFonte(fonte, apiKey = null) {
    const fonteConfig = typeof fonte === "string"
        ? RASTREAME_FONTES.find(item => item.nome === normalizarFonte(fonte))
        : fonte;
    const nomeFonte = fonteConfig?.nome || String(fonte || "").toUpperCase();
    const key = apiKey || obterApiKeyFonte(fonteConfig || {});

    if (!isApiKeyConfigurada(key)) {
        throw new Error("API key nao configurada.");
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), RASTREAME_TIMEOUT_MS);

    try {
        const resposta = await fetch(montarUrlRastreame(key), {
            method: "GET",
            signal: controller.signal
        });

        if (!resposta.ok) {
            throw new Error(`RastreaMe retornou HTTP ${resposta.status}.`);
        }

        const payload = await resposta.json();
        const itens = extrairItensResposta(payload);
        const veiculos = itens.map((item, index) => normalizarPosicaoRastreame(item, nomeFonte, index));

        return {
            fonte: nomeFonte,
            sucesso: true,
            total: veiculos.length,
            veiculos,
            camposDetectados: camposDisponiveis(itens[0]),
            primeiroItemSanitizado: sanitizarValor(itens[0] || {}),
            erro: null
        };
    } finally {
        clearTimeout(timeout);
    }
}

export async function buscarTodasPosicoesRastreame() {
    const fontes = await Promise.all(RASTREAME_FONTES.map(async (fonte) => {
        try {
            return await buscarPosicoesPorFonte(fonte);
        } catch (err) {
            return {
                fonte: fonte.nome,
                sucesso: false,
                total: 0,
                veiculos: [],
                camposDetectados: [],
                primeiroItemSanitizado: {},
                erro: err.name === "AbortError" ? "Tempo limite ao consultar RastreaMe." : err.message
            };
        }
    }));

    return {
        fontes,
        veiculos: fontes.flatMap(fonte => fonte.veiculos || [])
    };
}

export async function testarFontesRastreame() {
    const resultado = await buscarTodasPosicoesRastreame();
    return {
        fontes: resultado.fontes.map(fonte => ({
            fonte: fonte.fonte,
            sucesso: fonte.sucesso,
            total: fonte.total,
            camposDetectados: fonte.camposDetectados || [],
            primeiroItemSanitizado: fonte.primeiroItemSanitizado || {},
            erro: fonte.erro || null
        }))
    };
}

function criarStoreVinculosVazio() {
    return { nextId: 1, items: [] };
}

function normalizarStoreVinculos(store) {
    const items = (Array.isArray(store?.items) ? store.items : []).map((item, index) => ({
        id: Number(item.id) || index + 1,
        tecnico_id: String(item.tecnico_id || item.tecnicoId || "").trim(),
        fonte: normalizarFonte(item.fonte),
        veiculo_id: String(item.veiculo_id || item.veiculoId || "").trim(),
        placa: String(item.placa || "").trim().toUpperCase(),
        nome_veiculo: String(item.nome_veiculo || item.nomeVeiculo || "").trim(),
        nome_tecnico_api: String(item.nome_tecnico_api || item.nomeTecnicoApi || "").trim(),
        nome_motorista_api: String(item.nome_motorista_api || item.nomeMotoristaApi || item.nome_tecnico_api || item.nomeTecnicoApi || "").trim(),
        vinculo_origem: normalizarVinculoOrigem(item.vinculo_origem || item.vinculoOrigem, "manual"),
        confianca_vinculo: normalizarConfiancaVinculo(item.confianca_vinculo || item.confiancaVinculo, null),
        ultimo_match_em: item.ultimo_match_em || item.ultimoMatchEm || null,
        ativo: item.ativo !== false && item.ativo !== 0,
        created_at: item.created_at || new Date().toISOString(),
        updated_at: item.updated_at || item.created_at || new Date().toISOString()
    })).filter(item => item.tecnico_id && item.fonte && (item.veiculo_id || item.placa));
    const maiorId = items.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;
    return { nextId, items };
}

function lerStoreVinculos() {
    return normalizarStoreVinculos(lerJson(RASTREAME_VINCULOS_FILE, criarStoreVinculosVazio()));
}

function salvarStoreVinculos(store) {
    const normalizado = normalizarStoreVinculos(store);
    salvarJson(RASTREAME_VINCULOS_FILE, normalizado);
    return normalizado;
}

function criarStorePosicoesVazio() {
    return { nextId: 1, items: [] };
}

function normalizarStorePosicoes(store) {
    const items = (Array.isArray(store?.items) ? store.items : []).map((item, index) => {
        const rawJson = sanitizarValor(item.raw_json || item.raw || {});
        const coordenada = validarCoordenada(item.latitude, item.longitude);
        const dataPosicao = parseDataIso(item.data_posicao || item.dataPosicao);
        return {
            id: Number(item.id) || index + 1,
            fonte: normalizarFonte(item.fonte),
            veiculo_id: String(item.veiculo_id || item.veiculoId || "").trim(),
            placa: String(item.placa || "").trim().toUpperCase(),
            nome_veiculo: String(item.nome_veiculo || item.nomeVeiculo || "").trim(),
            nome_tecnico_api: String(item.nome_tecnico_api || item.nomeTecnicoApi || extrairNomeTecnicoRastreame(rawJson) || "").trim(),
            nome_motorista_api: String(item.nome_motorista_api || item.nomeMotoristaApi || item.nome_tecnico_api || item.nomeTecnicoApi || extrairNomeTecnicoRastreame(rawJson) || "").trim(),
            tecnico_id: item.tecnico_id ? String(item.tecnico_id).trim() : null,
            latitude: coordenada?.latitude ?? null,
            longitude: coordenada?.longitude ?? null,
            velocidade: parseNumero(item.velocidade),
            ignicao: parseBoolean(item.ignicao),
            endereco: String(item.endereco || "").trim(),
            data_posicao: dataPosicao,
            status_posicao: item.status_posicao || item.statusPosicao || obterStatusPosicaoGps(dataPosicao),
            vinculo_origem: normalizarVinculoOrigem(item.vinculo_origem || item.vinculoOrigem, item.tecnico_id ? "manual" : "sem_vinculo"),
            confianca_vinculo: normalizarConfiancaVinculo(item.confianca_vinculo || item.confiancaVinculo, null),
            raw_json: rawJson,
            created_at: item.created_at || new Date().toISOString(),
            updated_at: item.updated_at || item.created_at || new Date().toISOString()
        };
    }).filter(item => item.fonte && (item.veiculo_id || item.placa));
    const maiorId = items.reduce((maior, item) => Math.max(maior, item.id), 0);
    const nextId = Number.isInteger(store?.nextId) && store.nextId > maiorId ? store.nextId : maiorId + 1;
    return { nextId, items };
}

function lerStorePosicoes() {
    return normalizarStorePosicoes(lerJson(RASTREAME_POSICOES_FILE, criarStorePosicoesVazio()));
}

function salvarStorePosicoes(store) {
    const normalizado = normalizarStorePosicoes(store);
    salvarJson(RASTREAME_POSICOES_FILE, normalizado);
    return normalizado;
}

function lerStorePosicoesAtuais() {
    return normalizarStorePosicoes(lerJson(RASTREAME_POSICOES_ATUAIS_FILE, criarStorePosicoesVazio()));
}

function salvarStorePosicoesAtuais(store) {
    const normalizado = normalizarStorePosicoes(store);
    salvarJson(RASTREAME_POSICOES_ATUAIS_FILE, normalizado);
    return normalizado;
}

function obterVinculosAtivos() {
    return lerStoreVinculos().items.filter(item => item.ativo);
}

function obterVinculoAtivoVeiculo(veiculo, vinculos = obterVinculosAtivos()) {
    return vinculos.find(vinculo => mesmoVeiculo(vinculo, veiculo)) || null;
}

function obterVinculoAtivoTecnico(tecnicoId, vinculos = obterVinculosAtivos()) {
    return vinculos.find(vinculo => String(vinculo.tecnico_id) === String(tecnicoId)) || null;
}

function obterNomeTecnicoApiVeiculo(veiculo) {
    return String(
        veiculo?.nomeTecnicoRastreame
        || veiculo?.nomeTecnicoApi
        || veiculo?.nomeMotoristaApi
        || veiculo?.nome_tecnico_api
        || veiculo?.nome_motorista_api
        || extrairNomeTecnicoRastreame(veiculo?.raw_json || veiculo?.raw || {})
        || ""
    ).trim();
}

function detectarTecnicoApiVeiculo(veiculo, tecnicos = null) {
    const nomeTecnicoApi = obterNomeTecnicoApiVeiculo(veiculo);
    const match = encontrarTecnicoPorNome(nomeTecnicoApi, obterTecnicosAgendaRastreame(tecnicos));
    if (!match) {
        return {
            nomeTecnicoApi,
            match: null
        };
    }

    return {
        nomeTecnicoApi,
        match
    };
}

function atualizarVinculosAutomaticosPorNome(veiculos, tecnicos = null) {
    const listaTecnicos = obterTecnicosAgendaRastreame(tecnicos);
    const store = lerStoreVinculos();
    const agora = new Date().toISOString();
    let criados = 0;
    let atualizados = 0;
    let conflitosResolvidos = 0;
    let semMatch = 0;

    if (!listaTecnicos.length) {
        return { criados, atualizados, conflitosResolvidos, semMatch: veiculos.length };
    }

    veiculos.forEach(veiculo => {
        const { nomeTecnicoApi, match } = detectarTecnicoApiVeiculo(veiculo, listaTecnicos);
        if (!nomeTecnicoApi || !match) {
            semMatch += 1;
            return;
        }

        const referenciaVeiculo = {
            fonte: normalizarFonte(veiculo.fonte),
            veiculo_id: String(veiculo.veiculoId || veiculo.veiculo_id || "").trim(),
            placa: String(veiculo.placa || "").trim().toUpperCase(),
            nome_veiculo: String(veiculo.nomeVeiculo || veiculo.nome_veiculo || "").trim()
        };
        if (!referenciaVeiculo.fonte || (!referenciaVeiculo.veiculo_id && !referenciaVeiculo.placa)) return;

        store.items.forEach(item => {
            const mesmoTecnico = item.ativo && String(item.tecnico_id) === String(match.tecnicoId);
            const mesmoAlvoVeiculo = item.ativo && mesmoVeiculo(item, referenciaVeiculo);
            const mesmoRegistro = mesmoTecnico && mesmoAlvoVeiculo;
            if ((mesmoTecnico || mesmoAlvoVeiculo) && !mesmoRegistro) {
                item.ativo = false;
                item.updated_at = agora;
                conflitosResolvidos += 1;
            }
        });

        let item = store.items.find(registro => (
            String(registro.tecnico_id) === String(match.tecnicoId)
            && mesmoVeiculo(registro, referenciaVeiculo)
        ));

        if (!item) {
            item = {
                id: store.nextId,
                tecnico_id: String(match.tecnicoId),
                fonte: referenciaVeiculo.fonte,
                veiculo_id: referenciaVeiculo.veiculo_id,
                placa: referenciaVeiculo.placa,
                nome_veiculo: referenciaVeiculo.nome_veiculo,
                nome_tecnico_api: nomeTecnicoApi,
                nome_motorista_api: nomeTecnicoApi,
                vinculo_origem: "api_nome_tecnico",
                confianca_vinculo: match.confiancaVinculo,
                ultimo_match_em: agora,
                ativo: true,
                created_at: agora,
                updated_at: agora
            };
            store.nextId += 1;
            store.items.push(item);
            criados += 1;
            return;
        }

        item.ativo = true;
        item.fonte = referenciaVeiculo.fonte;
        item.veiculo_id = referenciaVeiculo.veiculo_id || item.veiculo_id;
        item.placa = referenciaVeiculo.placa || item.placa;
        item.nome_veiculo = referenciaVeiculo.nome_veiculo || item.nome_veiculo;
        item.nome_tecnico_api = nomeTecnicoApi;
        item.nome_motorista_api = nomeTecnicoApi;
        item.vinculo_origem = "api_nome_tecnico";
        item.confianca_vinculo = match.confiancaVinculo;
        item.ultimo_match_em = agora;
        item.updated_at = agora;
        atualizados += 1;
    });

    salvarStoreVinculos(store);
    return { criados, atualizados, conflitosResolvidos, semMatch };
}

function obterStatusPosicaoGps(dataPosicao) {
    if (!dataPosicao) return "sem_posicao";
    const timestamp = Date.parse(dataPosicao);
    if (!Number.isFinite(timestamp)) return "sem_posicao";
    const idadeMin = (Date.now() - timestamp) / 60000;
    if (idadeMin <= 2) return "tempo_real";
    if (idadeMin <= 15) return "atual";
    if (idadeMin <= 60) return "antiga";
    return "desatualizada";
}

function enriquecerVeiculoComVinculo(veiculo, vinculos = obterVinculosAtivos(), tecnicos = null) {
    const vinculo = obterVinculoAtivoVeiculo(veiculo, vinculos);
    const statusPosicaoGps = obterStatusPosicaoGps(veiculo.dataPosicao || veiculo.data_posicao);
    const { nomeTecnicoApi, match } = detectarTecnicoApiVeiculo(veiculo, tecnicos);
    const tecnicoId = match?.tecnicoId || vinculo?.tecnico_id || veiculo.tecnico_id || null;
    const tecnicoNome = match?.tecnicoNome || vinculo?.tecnico_id || veiculo.tecnico_id || null;
    const sugestaoVinculoTecnicoId = match?.tecnicoId || null;
    const sugestaoVinculoTecnicoNome = match?.tecnicoNome || null;
    const sugestaoVinculoAplicada = Boolean(sugestaoVinculoTecnicoId && String(sugestaoVinculoTecnicoId) === String(tecnicoId || ""));
    const vinculoOrigem = match
        ? "api_nome_tecnico"
        : (vinculo?.vinculo_origem || veiculo.vinculo_origem || (tecnicoId ? "manual" : "sem_vinculo"));
    const confiancaVinculo = match?.confiancaVinculo
        || vinculo?.confianca_vinculo
        || veiculo.confianca_vinculo
        || null;

    return {
        fonte: veiculo.fonte,
        veiculoId: veiculo.veiculoId || veiculo.veiculo_id,
        placa: veiculo.placa || "",
        nomeVeiculo: veiculo.nomeVeiculo || veiculo.nome_veiculo || "",
        nomeTecnicoRastreame: nomeTecnicoApi,
        nomeTecnicoApi,
        nomeMotoristaApi: nomeTecnicoApi,
        tecnicoDetectadoId: match?.tecnicoId || null,
        tecnicoDetectadoNome: match?.tecnicoNome || null,
        sugestaoVinculoTecnicoId,
        sugestaoVinculoTecnicoNome,
        sugestaoVinculoConfianca: match?.confiancaVinculo || null,
        sugestaoVinculoAplicada,
        latitude: veiculo.latitude,
        longitude: veiculo.longitude,
        velocidade: veiculo.velocidade,
        ignicao: veiculo.ignicao,
        endereco: veiculo.endereco || "",
        dataPosicao: veiculo.dataPosicao || veiculo.data_posicao || null,
        tecnicoId,
        tecnicoNome,
        vinculoId: vinculo?.id || null,
        vinculoOrigem,
        confiancaVinculo,
        statusPosicaoGps
    };
}

function salvarPosicoesRecebidas(veiculos, { tecnicos = null } = {}) {
    const autoVinculosNome = atualizarVinculosAutomaticosPorNome(veiculos, tecnicos);
    const store = lerStorePosicoes();
    const storeAtual = lerStorePosicoesAtuais();
    const vinculos = obterVinculosAtivos();
    const agora = new Date().toISOString();
    let novas = 0;
    let vinculadas = 0;
    let atualizadas = 0;

    veiculos.forEach(veiculo => {
        const veiculoEnriquecido = enriquecerVeiculoComVinculo(veiculo, vinculos, tecnicos);
        if (veiculoEnriquecido.tecnicoId) vinculadas += 1;

        const registro = {
            id: store.nextId,
            fonte: veiculo.fonte,
            veiculo_id: veiculo.veiculoId,
            placa: veiculo.placa,
            nome_veiculo: veiculo.nomeVeiculo,
            nome_tecnico_api: veiculoEnriquecido.nomeTecnicoRastreame || "",
            nome_motorista_api: veiculoEnriquecido.nomeMotoristaApi || "",
            tecnico_id: veiculoEnriquecido.tecnicoId || null,
            latitude: veiculo.latitude,
            longitude: veiculo.longitude,
            velocidade: veiculo.velocidade,
            ignicao: veiculo.ignicao,
            endereco: veiculo.endereco,
            data_posicao: veiculo.dataPosicao,
            status_posicao: obterStatusPosicaoGps(veiculo.dataPosicao),
            vinculo_origem: veiculoEnriquecido.vinculoOrigem || "sem_vinculo",
            confianca_vinculo: veiculoEnriquecido.confiancaVinculo || null,
            raw_json: veiculo.raw || {},
            created_at: agora,
            updated_at: agora
        };
        const duplicada = store.items.find(item => (
            mesmoVeiculo(item, registro)
            && String(item.data_posicao || "") === String(registro.data_posicao || "")
            && Number(item.latitude) === Number(registro.latitude)
            && Number(item.longitude) === Number(registro.longitude)
        ));

        if (duplicada) {
            const camposAtualizaveis = [
                "nome_tecnico_api",
                "nome_motorista_api",
                "tecnico_id",
                "vinculo_origem",
                "confianca_vinculo",
                "placa",
                "nome_veiculo",
                "status_posicao"
            ];
            let mudou = false;
            camposAtualizaveis.forEach(campo => {
                if (String(duplicada[campo] || "") === String(registro[campo] || "")) return;
                duplicada[campo] = registro[campo];
                mudou = true;
            });
            if (mudou) duplicada.updated_at = agora;
            if (mudou) atualizadas += 1;
        } else {
            store.items.push(registro);
            store.nextId += 1;
            novas += 1;
        }

        const atual = storeAtual.items.find(item => mesmoVeiculo(item, registro));
        if (atual) {
            Object.assign(atual, {
                ...registro,
                id: atual.id,
                created_at: atual.created_at || registro.created_at,
                updated_at: agora
            });
        } else {
            storeAtual.items.push({
                ...registro,
                id: storeAtual.nextId,
                created_at: agora,
                updated_at: agora
            });
            storeAtual.nextId += 1;
        }
    });

    store.items = store.items.slice(-4000);
    salvarStorePosicoes(store);
    salvarStorePosicoesAtuais(storeAtual);

    return { novas, vinculadas, atualizadas, autoVinculosNome };
}

function obterUltimasPosicoes() {
    const atuais = lerStorePosicoesAtuais();
    if (atuais.items.length) {
        return atuais.items.map(item => ({
            ...item,
            status_posicao: obterStatusPosicaoGps(item.data_posicao)
        }));
    }

    const store = lerStorePosicoes();
    const porVeiculo = new Map();

    store.items.forEach(item => {
        const chave = chaveVeiculo({ fonte: item.fonte, veiculoId: item.veiculo_id, placa: item.placa });
        const atual = porVeiculo.get(chave);
        const novoTimestamp = Date.parse(item.data_posicao || item.created_at) || 0;
        const atualTimestamp = atual ? (Date.parse(atual.data_posicao || atual.created_at) || 0) : Number.NEGATIVE_INFINITY;

        if (!atual || novoTimestamp >= atualTimestamp) {
            porVeiculo.set(chave, item);
        }
    });

    return [...porVeiculo.values()];
}

function contarResumoVeiculos(veiculos) {
    const porFonte = Object.fromEntries(RASTREAME_FONTES.map(fonte => [fonte.nome, 0]));
    veiculos.forEach(veiculo => {
        if (porFonte[veiculo.fonte] !== undefined) porFonte[veiculo.fonte] += 1;
    });

    return {
        total: veiculos.length,
        porFonte,
        vinculados: veiculos.filter(veiculo => veiculo.tecnicoId).length,
        semVinculo: veiculos.filter(veiculo => !veiculo.tecnicoId).length,
        gpsTempoReal: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "tempo_real").length,
        gpsAtual: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "atual").length,
        gpsAntigo: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "antiga").length,
        gpsDesatualizado: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "desatualizada").length,
        semPosicao: veiculos.filter(veiculo => veiculo.statusPosicaoGps === "sem_posicao").length
    };
}

export async function sincronizarPosicoesRastreame({ force = false, tecnicos = null } = {}) {
    if (tecnicos) configurarTecnicosAgendaRastreame(tecnicos);
    const tecnicosAgenda = obterTecnicosAgendaRastreame(tecnicos);
    const agoraMs = Date.now();

    if (!force && lastSyncResult && (agoraMs - lastSyncRastreameAt) < RASTREAME_CACHE_TTL_MS) {
        return lastSyncResult;
    }

    if (isSyncingRastreame) {
        return syncPromise || lastSyncResult || { sucesso: true, sincronizando: true, fontes: [], totalVeiculos: 0 };
    }

    isSyncingRastreame = true;
    syncPromise = (async () => {
        const consulta = await buscarTodasPosicoesRastreame();
        const salvos = salvarPosicoesRecebidas(consulta.veiculos, { tecnicos: tecnicosAgenda });
        const vinculos = obterVinculosAtivos();
        const veiculos = consulta.veiculos.map(veiculo => enriquecerVeiculoComVinculo(veiculo, vinculos, tecnicosAgenda));
        const fontes = consulta.fontes.map(fonte => {
            const veiculosFonte = veiculos.filter(veiculo => veiculo.fonte === fonte.fonte);
            return {
                fonte: fonte.fonte,
                sucesso: fonte.sucesso,
                veiculosEncontrados: fonte.total || 0,
                veiculosVinculados: veiculosFonte.filter(veiculo => veiculo.tecnicoId).length,
                semVinculo: veiculosFonte.filter(veiculo => !veiculo.tecnicoId).length,
                erro: fonte.erro || null
            };
        });

        lastSyncRastreameAt = Date.now();
        lastSyncRastreameError = null;
        lastSyncResult = {
            sucesso: true,
            fontes,
            totalVeiculos: veiculos.length,
            salvas: salvos.novas,
            atualizadas: salvos.atualizadas,
            vinculadas: salvos.vinculadas,
            autoVinculosNome: salvos.autoVinculosNome,
            sincronizadoEm: new Date(lastSyncRastreameAt).toISOString(),
            veiculos
        };

        return lastSyncResult;
    })();

    try {
        return await syncPromise;
    } catch (err) {
        lastSyncRastreameError = err.message || "Erro ao sincronizar RastreaMe.";
        throw err;
    } finally {
        isSyncingRastreame = false;
        syncPromise = null;
    }
}

export function iniciarSincronizacaoRastreameSeNecessario({ tecnicos = null } = {}) {
    if (tecnicos) configurarTecnicosAgendaRastreame(tecnicos);
    if (isSyncingRastreame) return false;
    if (Date.now() - lastSyncRastreameAt < RASTREAME_CACHE_TTL_MS) return false;

    sincronizarPosicoesRastreame({ force: false, tecnicos }).catch(err => {
        console.error("Erro ao sincronizar RastreaMe em background:", err.message);
    });
    return true;
}

export function obterVeiculosRastreame({ tecnicos = null } = {}) {
    if (tecnicos) configurarTecnicosAgendaRastreame(tecnicos);
    const tecnicosAgenda = obterTecnicosAgendaRastreame(tecnicos);
    const vinculos = obterVinculosAtivos();
    const veiculos = obterUltimasPosicoes().map(item => enriquecerVeiculoComVinculo(item, vinculos, tecnicosAgenda));
    const resumo = contarResumoVeiculos(veiculos);
    const fontesUltimoSync = new Map((lastSyncResult?.fontes || []).map(fonte => [fonte.fonte, fonte]));

    return {
        fontes: RASTREAME_FONTES.map(fonte => ({
            nome: fonte.nome,
            total: resumo.porFonte[fonte.nome] || 0,
            erro: fontesUltimoSync.get(fonte.nome)?.erro || null
        })),
        veiculos,
        resumo,
        sincronizadoEm: lastSyncRastreameAt ? new Date(lastSyncRastreameAt).toISOString() : null,
        sincronizando: isSyncingRastreame,
        erro: lastSyncRastreameError
    };
}

export function obterMapaPosicoesTecnicosRastreame({ tecnicos = null } = {}) {
    const mapa = new Map();

    obterVeiculosRastreame({ tecnicos }).veiculos
        .filter(veiculo => veiculo.tecnicoId)
        .filter(veiculo => validarCoordenada(veiculo.latitude, veiculo.longitude))
        .forEach(veiculo => {
            const atual = mapa.get(veiculo.tecnicoId);
            const novoTimestamp = Date.parse(veiculo.dataPosicao || "") || 0;
            const atualTimestamp = atual ? (Date.parse(atual.dataPosicao || "") || 0) : Number.NEGATIVE_INFINITY;
            if (!atual || novoTimestamp >= atualTimestamp) {
                mapa.set(veiculo.tecnicoId, veiculo);
            }
        });

    return mapa;
}

export function obterPosicaoAtualPorTecnico(tecnicoId) {
    return obterMapaPosicoesTecnicosRastreame().get(String(tecnicoId)) || null;
}

export function obterPosicoesAtualizadasRastreame({ tecnicos = null } = {}) {
    const veiculosDados = obterVeiculosRastreame({ tecnicos });
    const tecnicosGps = [...obterMapaPosicoesTecnicosRastreame({ tecnicos }).entries()].map(([tecnicoId, veiculo]) => ({
        tecnicoId,
        tecnicoNome: veiculo.tecnicoNome || tecnicoId,
        latitude: veiculo.latitude,
        longitude: veiculo.longitude,
        fonteLocalizacao: "gps_rastreame",
        empresaRastreame: veiculo.fonte,
        ultimaAtualizacaoLocalizacao: veiculo.dataPosicao,
        statusPosicaoGps: veiculo.statusPosicaoGps,
        vinculoRastreameOrigem: veiculo.vinculoOrigem,
        veiculo
    }));

    return {
        tecnicos: tecnicosGps,
        veiculos: veiculosDados.veiculos,
        resumo: veiculosDados.resumo,
        fontes: veiculosDados.fontes,
        sincronizadoEm: veiculosDados.sincronizadoEm,
        sincronizando: veiculosDados.sincronizando,
        erro: veiculosDados.erro || null
    };
}

export function vincularVeiculoTecnico({ tecnicoId, fonte, veiculoId, placa, nomeVeiculo, nomeTecnicoApi = "", confirmarTroca = false }) {
    const tecnico = String(tecnicoId || "").trim();
    const fonteNormalizada = normalizarFonte(fonte);
    const veiculo = {
        fonte: fonteNormalizada,
        veiculo_id: String(veiculoId || "").trim(),
        placa: String(placa || "").trim().toUpperCase(),
        nome_veiculo: String(nomeVeiculo || "").trim()
    };

    if (!tecnico) {
        const erro = new Error("Tecnico nao informado.");
        erro.statusCode = 400;
        throw erro;
    }
    if (!fonteNormalizada || (!veiculo.veiculo_id && !veiculo.placa)) {
        const erro = new Error("Veiculo RastreaMe invalido.");
        erro.statusCode = 400;
        throw erro;
    }

    const store = lerStoreVinculos();
    const conflito = store.items.find(item => item.ativo && item.tecnico_id !== tecnico && mesmoVeiculo(item, veiculo));

    if (conflito && !confirmarTroca) {
        const erro = new Error(`Veiculo ja vinculado ao tecnico ${conflito.tecnico_id}.`);
        erro.statusCode = 409;
        erro.conflito = conflito;
        throw erro;
    }

    const agora = new Date().toISOString();
    store.items.forEach(item => {
        const deveDesativar = item.ativo && (
            item.tecnico_id === tecnico ||
            (confirmarTroca && mesmoVeiculo(item, veiculo))
        );
        if (deveDesativar) {
            item.ativo = false;
            item.updated_at = agora;
        }
    });

    let item = store.items.find(registro => registro.tecnico_id === tecnico && mesmoVeiculo(registro, veiculo));
    if (!item) {
        item = {
            id: store.nextId,
            tecnico_id: tecnico,
            fonte: fonteNormalizada,
            veiculo_id: veiculo.veiculo_id,
            placa: veiculo.placa,
            nome_veiculo: veiculo.nome_veiculo,
            nome_tecnico_api: String(nomeTecnicoApi || "").trim(),
            vinculo_origem: "manual",
            confianca_vinculo: "alta",
            ultimo_match_em: null,
            ativo: true,
            created_at: agora,
            updated_at: agora
        };
        store.nextId += 1;
        store.items.push(item);
    } else {
        item.ativo = true;
        item.placa = veiculo.placa || item.placa;
        item.nome_veiculo = veiculo.nome_veiculo || item.nome_veiculo;
        item.nome_tecnico_api = String(nomeTecnicoApi || item.nome_tecnico_api || "").trim();
        item.vinculo_origem = "manual";
        item.confianca_vinculo = "alta";
        item.updated_at = agora;
    }

    return {
        vinculo: item,
        vinculos: salvarStoreVinculos(store).items.filter(registro => registro.ativo)
    };
}

export function desvincularVeiculoTecnico({ tecnicoId, fonte = "", veiculoId = "", placa = "" }) {
    const tecnico = String(tecnicoId || "").trim();
    const store = lerStoreVinculos();
    const alvo = {
        fonte: normalizarFonte(fonte),
        veiculo_id: String(veiculoId || "").trim(),
        placa: String(placa || "").trim().toUpperCase()
    };
    const agora = new Date().toISOString();
    let alterados = 0;

    store.items.forEach(item => {
        const matchTecnico = tecnico && item.tecnico_id === tecnico;
        const matchVeiculo = alvo.fonte && mesmoVeiculo(item, alvo);
        if (item.ativo && (matchTecnico || matchVeiculo)) {
            item.ativo = false;
            item.updated_at = agora;
            alterados += 1;
        }
    });

    return {
        alterados,
        vinculos: salvarStoreVinculos(store).items.filter(registro => registro.ativo)
    };
}
