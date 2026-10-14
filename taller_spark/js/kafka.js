const indicadorEstado = document.getElementById("estado-conexion");
const contenedorTopicos = document.getElementById("topicos");
const cuerpoMensajes = document.getElementById("cuerpo-mensajes");
const tablaMensajes = document.getElementById("tabla-mensajes");
const botonPausa = document.getElementById("btn-pausa");
const checkCrudo = document.getElementById("ver-crudo");

// Etiqueta de cada tópico (si cambias los nombres de los tópicos, cámbialos aquí también)
const ETIQUETAS = {
    "telemetria-drones":    { productor: "Productor 1", detalle: "Drones de entrega" },
    "telemetria-satelites": { productor: "Productor 2", detalle: "Satélites LEO" },
};

const MAX_FILAS = 80;          // filas que se conservan en la tabla
const VENTANA_TASA_MS = 5000;  // los mensajes/s se calculan sobre los últimos 5 segundos

const tarjetas = {};   // tópico -> elementos de su tarjeta + llegadas recientes
let pausado = false;
let ultimoError = null;

// --- Indicador de conexión ---

function mostrarEstado(texto, clase) {
    indicadorEstado.textContent = texto;
    indicadorEstado.className = clase;
}

// --- Tarjetas de tópicos (se crean una vez; luego solo se actualizan sus textos) ---

function obtenerTarjeta(topico) {
    if (tarjetas[topico]) return tarjetas[topico];

    const etiqueta = ETIQUETAS[topico] || { productor: "Productor", detalle: "" };

    const el = document.createElement("article");
    el.className = "tarjeta-topico";
    el.innerHTML = `
        <div class="tarjeta-titulo"><span class="pulso"></span><strong></strong></div>
        <div class="tarjeta-sub"></div>
        <div class="tarjeta-tasa"><span class="tasa-valor">0.0</span> <small>mensajes/s</small></div>
        <div class="tarjeta-total">Almacenados en el tópico: <span class="total-valor">0</span></div>
        <div class="tarjeta-particiones"></div>
    `;
    el.querySelector("strong").textContent = topico;
    el.querySelector(".tarjeta-sub").textContent = `${etiqueta.productor} · ${etiqueta.detalle}`;
    contenedorTopicos.appendChild(el);

    tarjetas[topico] = {
        pulso: el.querySelector(".pulso"),
        tasa: el.querySelector(".tasa-valor"),
        total: el.querySelector(".total-valor"),
        contParticiones: el.querySelector(".tarjeta-particiones"),
        chips: {},
        llegadas: [],   // [{t, n}] mensajes recibidos en cada paquete, para calcular mensajes/s
    };
    return tarjetas[topico];
}

function actualizarParticiones(tarjeta, particiones) {
    Object.entries(particiones).forEach(([id, offsetFinal]) => {
        let chip = tarjeta.chips[id];
        if (!chip) {
            chip = document.createElement("span");
            chip.className = "chip-particion";
            tarjeta.contParticiones.appendChild(chip);
            tarjeta.chips[id] = chip;
        }
        chip.textContent = `P${id} · offset ${offsetFinal.toLocaleString("es-CO")}`;
    });
}

function hacerPulso(tarjeta) {
    tarjeta.pulso.classList.add("activo");
    setTimeout(() => tarjeta.pulso.classList.remove("activo"), 400);
}

// --- Tabla de mensajes ---

function resumirDatos(v) {
    if (v.crudo !== undefined) return v.crudo;
    try {
        const altitud = v.tipo === "satelite"
            ? `${(v.altitud / 1000).toFixed(1)} km`
            : `${v.altitud} m`;
        return `alt ${altitud} · vel ${Math.round(v.velocidad)} km/h · ` +
               `rumbo ${Math.round(v.rumbo)}° · bat ${Number(v.bateria).toFixed(1)}%`;
    } catch (e) {
        return JSON.stringify(v);
    }
}

function agregarFila(m) {
    const indiceTopico = Math.max(0, Object.keys(tarjetas).indexOf(m.topico));

    const tr = document.createElement("tr");
    tr.className = `fila-nueva topico-${indiceTopico % 2}`;

    const hora = new Date(m.marca_ms).toLocaleTimeString("es-CO", { hour12: false });
    [hora, m.topico, `P${m.particion}`, m.offset, m.clave ?? "—"].forEach(texto => {
        const td = document.createElement("td");
        td.textContent = texto;
        tr.appendChild(td);
    });

    const tdDatos = document.createElement("td");
    const resumen = document.createElement("span");
    resumen.className = "resumen";
    resumen.textContent = resumirDatos(m.valor || {});
    const crudo = document.createElement("span");
    crudo.className = "crudo";
    crudo.textContent = JSON.stringify(m.valor);
    tdDatos.append(resumen, crudo);
    tr.appendChild(tdDatos);

    cuerpoMensajes.prepend(tr);
    while (cuerpoMensajes.children.length > MAX_FILAS) {
        cuerpoMensajes.removeChild(cuerpoMensajes.lastChild);
    }
}

// --- Salida de Spark hacia MongoDB ---

function actualizarDestino(destino) {
    if (!destino) return;
    document.getElementById("dest-lecturas").textContent = destino.lecturas.toLocaleString("es-CO");
    document.getElementById("dest-alertas").textContent = destino.alertas.toLocaleString("es-CO");
    document.getElementById("dest-lote").textContent = destino.ultimo_lote || "ninguno aún";
}

// --- Recepción de cada paquete que empuja el servidor ---

function recibir(paquete) {
    if (paquete.error) {
        ultimoError = paquete.error;
        mostrarEstado(`● ${paquete.error} (reintentando...)`, "estado-error");
        return;
    }
    ultimoError = null;
    mostrarEstado("● En vivo: leyendo mensajes de Kafka", "estado-vivo");

    // Una tarjeta por tópico, con su total y los offsets de sus particiones
    Object.entries(paquete.topicos || {}).forEach(([topico, info]) => {
        const tarjeta = obtenerTarjeta(topico);
        tarjeta.total.textContent = info.total.toLocaleString("es-CO");
        actualizarParticiones(tarjeta, info.particiones);
    });

    // Mensajes nuevos: a la tabla (si no está en pausa) y al conteo de mensajes/s
    const porTopico = {};
    (paquete.mensajes || []).forEach(m => {
        porTopico[m.topico] = (porTopico[m.topico] || 0) + 1;
        if (!pausado) agregarFila(m);
    });

    const ahora = performance.now();
    Object.entries(tarjetas).forEach(([topico, tarjeta]) => {
        const n = porTopico[topico] || 0;
        if (n > 0) {
            tarjeta.llegadas.push({ t: ahora, n });
            hacerPulso(tarjeta);
        }
        while (tarjeta.llegadas.length && ahora - tarjeta.llegadas[0].t > VENTANA_TASA_MS) {
            tarjeta.llegadas.shift();
        }
        const suma = tarjeta.llegadas.reduce((acum, x) => acum + x.n, 0);
        tarjeta.tasa.textContent = (suma / (VENTANA_TASA_MS / 1000)).toFixed(1);
    });

    actualizarDestino(paquete.destino);
}

// --- Conexión: el servidor EMPUJA los datos (Server-Sent Events) ---

function conectar() {
    mostrarEstado("● Conectando con el servidor...", "estado-espera");
    const fuente = new EventSource("/api/kafka-stream");

    fuente.onopen = () => {
        mostrarEstado("● Conectado, leyendo Kafka...", "estado-espera");
    };

    fuente.onmessage = (mensaje) => {
        try {
            recibir(JSON.parse(mensaje.data));
        } catch (e) {
            console.warn("Mensaje del stream no válido", e);
        }
    };

    fuente.onerror = () => {
        const texto = ultimoError
            ? `● ${ultimoError} (reintentando...)`
            : "● Sin conexión con el servidor (¿api.py está corriendo? ¿abriste localhost:5000/kafka?)";
        mostrarEstado(texto, "estado-error");
    };
}

botonPausa.addEventListener("click", () => {
    pausado = !pausado;
    botonPausa.textContent = pausado ? "▶ Reanudar" : "⏸ Pausar";
});

checkCrudo.addEventListener("change", () => {
    tablaMensajes.classList.toggle("modo-crudo", checkCrudo.checked);
});

conectar();