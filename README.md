# Agenda de Técnicos – Acessanet Telecom

Painel operacional (NOC) para gestão da agenda dos técnicos de campo da **Acessanet Telecom**: acompanhamento de ordens de serviço, rastreamento de equipes em tempo real, coordenação de rotas e indicadores operacionais.

## Funcionalidades

### Agenda de Técnicos
- Agenda de instalação com horários, clientes e status das ordens de serviço (pendente, em execução, concluída)
- Horários da semana por técnico, configuração de turnos e marcação de almoço
- Gestão de ausências e técnicos responsáveis por tipo de serviço
- Histórico de chamados massivos e reagendamento de ordens de serviço
- Cores de equipe por técnico

### Coordenador de Rotas
- Mapa de chamados em aberto, com geocodificação automática de endereços
- GPS dos técnicos em tempo real (vinculação/desvinculação de veículos de rastreamento)
- Sugestão e aplicação automática de rotas, com comparação entre rotas
- Desenhos de área no mapa (ferramentas de caneta/borracha) para planejamento de regiões
- Pontos fixos e gerenciamento de endereços não localizados

### Indicadores e alertas
- Chamados críticos, em execução simultânea e possíveis incidentes regionais
- Distribuição e impacto por bairro, sobrecarga de agenda e ranking operacional
- Alertas de pendências de localização e sincronização com o SGP

### Chat interno
- Lista de usuários e comunicação interna da equipe do NOC

## Em desenvolvimento

- **Mover técnico pelo mouse (drag and drop):** a interação de arrastar e soltar já está implementada no frontend, mas ainda está aguardando a API de `POST` correspondente no backend para persistir a movimentação. Até essa API ser implementada, a movimentação não é salva de fato.

## Tecnologias

- **Frontend:** HTML, CSS e JavaScript
- **Backend:** Node.js + Express, Socket.IO (atualizações em tempo real)
- **Banco de dados:** MariaDB
- **Testes:** Playwright

## Estrutura do projeto

```
api/
├── index.js            # Servidor Express / rotas da API / Socket.IO
├── services/           # Regras de negócio (agenda, rastreamento, mapa, pontos fixos, etc.)
├── migrations/         # Scripts SQL de migração do banco
├── public/             # Frontend (painel NOC)
└── data/               # Dados operacionais em runtime (não versionado)
```

## Como usar

1. Clone o repositório:
```bash
git clone git@github.com:Olukasao/Agenda-tecnicos.git
cd Agenda-tecnicos
```

2. Instale as dependências:
```bash
npm install
```

3. Configure as variáveis de ambiente (`.env`) e os certificados (`cert.pem`/`key.pem`) necessários para rodar em HTTPS.

4. Execute o backend:
```bash
npm run dev
```

5. Acesse `https://localhost:8500/` (ou `https://{IP}:8500/`).
