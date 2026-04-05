# Modo: batch — Procesamiento Masivo de Ofertas

Dos modos de uso: **conductor --chrome** (navega portales en tiempo real) o **standalone** (script para URLs ya recolectadas).

## Arquitectura

### Opción A: Paperclip Agent (Recomendado)

Si usas Paperclip para FluxSpeak, CareerOps puede correr como otro agente autónomo en el mismo daemon:

```
Paperclip Daemon (heartbeat cada hora)
  │
  ├─ Research Agent → FluxSpeak
  ├─ Content Agent → FluxSpeak
  ├─ CMO Agent → FluxSpeak
  └─ 💼 CareerOps Agent → career-ops
       │
       └─ Corre una vez al día
           └─ Si hay ofertas pendientes:
               └─ Ejecuta batch/batch-orchestrator.py
                   └─ Hermes chat -q --skills career-ops
                       └─ Evaluación A-F + PDF + tracker
```

**Ventajas:**
- Un solo daemon 24/7
- No conflictos de PID
- CareerOps solo se activa cuando hay ofertas pendientes
- No interfiere con FluxSpeak

### Opción B: Hermes Conductor Manual

```
Hermes Conductor (navega portales con Playwright)
  │
  ├─ Oferta 1: lee JD del DOM + URL
  │    └─► delegate_task worker → report .md + PDF + tracker line
  │
  └─ Fin: merge tracker-additions → applications.md + resumen
```

## Archivos

```
batch/
  batch-input.tsv               # URLs (por conductor o manual)
  batch-state.tsv               # Progreso (auto-generado, gitignored)
  batch-runner.sh               # Script orquestador standalone
  batch-prompt.md               # Prompt template para workers
  logs/                         # Un log por oferta (gitignored)
  tracker-additions/            # Líneas de tracker (gitignored)
```

## Modo A: Conductor --chrome

1. **Leer estado**: `batch/batch-state.tsv` → saber qué ya se procesó
2. **Navegar portal**: Chrome → URL de búsqueda
3. **Extraer URLs**: Leer DOM de resultados → extraer lista de URLs → append a `batch-input.tsv`
4. **Para cada URL pendiente**:
   a. Chrome: click en la oferta → leer JD text del DOM
   b. Guardar JD a `/tmp/batch-jd-{id}.txt`
   c. Calcular siguiente REPORT_NUM secuencial
   d. Ejecutar via Bash:
      ```bash
      hermes -p --dangerously-skip-permissions \
        --append-system-prompt-file batch/batch-prompt.md \
        "Procesa esta oferta. URL: {url}. JD: /tmp/batch-jd-{id}.txt. Report: {num}. ID: {id}"
      ```
   e. Actualizar `batch-state.tsv` (completed/failed + score + report_num)
   f. Log a `logs/{report_num}-{id}.log`
   g. Chrome: volver atrás → siguiente oferta
5. **Paginación**: Si no hay más ofertas → click "Next" → repetir
6. **Fin**: Merge `tracker-additions/` → `applications.md` + resumen

## Modo B: Batch Orchestrator (Hermes nativo)

```bash
batch/batch-orchestrator.py [OPTIONS]
```

El orquestador usa `hermes chat -q --skills career-ops` para procesar cada oferta. No necesitas `hermes -p` ni `claude -p`.

Opciones:
- `--dry-run` — lista pendientes sin ejecutar
- `--retry-failed` — solo reintenta fallidas
- `--start-from N` — empieza desde ID N
- `--parallel N` — N workers en paralelo vía asyncio
- `--max-retries N` — intentos por oferta (default: 2)

### Desde Paperclip

Si ya tienes Paperclip corriendo para FluxSpeak, añade ofertas a `batch/batch-input.tsv` y el `CareerOpsAgent` las procesará automáticamente (máximo una vez al día para no saturar).

## Formato batch-state.tsv

```
id	url	status	started_at	completed_at	report_num	score	error	retries
1	https://...	completed	2026-...	2026-...	002	4.2	-	0
2	https://...	failed	2026-...	2026-...	-	-	Error msg	1
3	https://...	pending	-	-	-	-	-	0
```

## Resumabilidad

- Si muere → re-ejecutar → lee `batch-state.tsv` → skip completadas
- Lock file (`batch-runner.pid`) previene ejecución doble
- Cada worker es independiente: fallo en oferta #47 no afecta a las demás

## Workers (Hermes Agent)

Cada worker es un proceso `hermes chat -q --skills career-ops --toolsets terminal,file,web` que recibe el prompt completo de `batch-prompt.md` + los datos de la oferta. Es self-contained.

El worker produce:
1. Report `.md` en `reports/`
2. PDF en `output/`
3. Línea de tracker en `batch/tracker-additions/{id}.tsv`
4. JSON de resultado por stdout

## Gestión de errores

| Error | Recovery |
|-------|----------|
| URL inaccesible | Worker falla → conductor marca `failed`, siguiente |
| JD detrás de login | Conductor intenta leer DOM. Si falla → `failed` |
| Portal cambia layout | Conductor razona sobre HTML, se adapta |
| Worker crashea | Conductor marca `failed`, siguiente. Retry con `--retry-failed` |
| Conductor muere | Re-ejecutar → lee state → skip completadas |
| PDF falla | Report .md se guarda. PDF queda pendiente |
