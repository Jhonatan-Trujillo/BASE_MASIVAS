const canvas = document.getElementById("mapa");
const ctx = canvas.getContext("2d");
const canvasFondo = document.getElementById("mapa-fondo");
const ctxFondo = canvasFondo.getContext("2d");

const listaAlertas = document.getElementById("lista-alertas");
const listaSatelites = document.getElementById("lista-satelites");

const LAT_MIN = 4.40, LAT_MAX = 4.46;
const LON_MIN = -75.26, LON_MAX = -75.20;

const idsVistos = new Set();
const historialPosiciones = {}; // id -> [{x,y}, ...] para la estela

// Estaciones ADS-B fijas (decorativas, representan al "Productor" del enunciado)
const estaciones = [
    { nombre: "Estación Norte", x: 60, y: 50 },
    { nombre: "Estación Sur", x: 640, y: 450 },
    { nombre: "Estación Este", x: 640, y: 60 },
];

function proyectar(lat, lon) {
    const x = ((lon - LON_MIN) / (LON_MAX - LON_MIN)) * canvas.width;
    const y = ((LAT_MAX - lat) / (LAT_MAX - LAT_MIN)) * canvas.height;
    return { x, y };
}

// --- Capa de FONDO: se dibuja una sola vez, no se repite en cada actualización ---

function dibujarFondoRadar() {
    const cx = canvasFondo.width / 2;
    const cy = canvasFondo.height / 2;
    ctxFondo.strokeStyle = "rgba(56, 189, 248, 0.08)";
    ctxFondo.lineWidth = 1;
    for (let r = 60; r < canvasFondo.width; r += 60) {
        ctxFondo.beginPath();
        ctxFondo.arc(cx, cy, r, 0, Math.PI * 2);
        ctxFondo.stroke();
    }
    ctxFondo.beginPath();
    ctxFondo.moveTo(0, cy); ctxFondo.lineTo(canvasFondo.width, cy);
    ctxFondo.moveTo(cx, 0); ctxFondo.lineTo(cx, canvasFondo.height);
    ctxFondo.stroke();
}

function dibujarEstaciones() {
    estaciones.forEach(e => {
        ctxFondo.fillStyle = "#a78bfa";
        ctxFondo.beginPath();
        ctxFondo.arc(e.x, e.y, 5, 0, Math.PI * 2);
        ctxFondo.fill();
        ctxFondo.strokeStyle = "rgba(167, 139, 250, 0.4)";
        ctxFondo.beginPath();
        ctxFondo.arc(e.x, e.y, 14, 0, Math.PI * 2);
        ctxFondo.stroke();
        ctxFondo.fillStyle = "#c4b5fd";
        ctxFondo.font = "9px sans-serif";
        ctxFondo.fillText(e.nombre, e.x + 8, e.y - 8);
    });
}

// --- Capa DINÁMICA: se redibuja en cada ciclo (drones + estelas) ---

function dibujarEstela(id, x, y) {
    if (!historialPosiciones[id]) historialPosiciones[id] = [];
    const puntos = historialPosiciones[id];
    puntos.push({ x, y });
    if (puntos.length > 15) puntos.shift();

    ctx.beginPath();
    puntos.forEach((p, i) => {
        ctx.globalAlpha = (i / puntos.length) * 0.5;
        if (i === 0) ctx.moveTo(p.x, p.y);
        else ctx.lineTo(p.x, p.y);
    });
    ctx.strokeStyle = "#38bdf8";
    ctx.lineWidth = 2;
    ctx.stroke();
    ctx.globalAlpha = 1;
}

function dibujarDron(v) {
    const { x, y } = proyectar(v.lat, v.lon);
    dibujarEstela(v.id, x, y);

    const tieneAlerta = v.alerta_bateria || v.alerta_altitud || v.alerta_velocidad;

    ctx.save();
    ctx.translate(x, y);
    ctx.rotate((v.rumbo || 0) * Math.PI / 180);

    ctx.beginPath();
    ctx.moveTo(0, -8);
    ctx.lineTo(6, 6);
    ctx.lineTo(-6, 6);
    ctx.closePath();
    ctx.fillStyle = tieneAlerta ? "#ef4444" : "#38bdf8";
    ctx.fill();
    ctx.restore();

    ctx.fillStyle = "#e5e7eb";
    ctx.font = "10px sans-serif";
    ctx.fillText(`${v.id} (${Math.round(v.bateria)}%)`, x + 10, y + 3);

    if (tieneAlerta) {
        ctx.beginPath();
        ctx.strokeStyle = "#ef4444";
        ctx.lineWidth = 1.5;
        ctx.arc(x, y, 14, 0, Math.PI * 2);
        ctx.stroke();
    }
}

function actualizarPanelSatelites(satelites) {
    listaSatelites.innerHTML = "";
    satelites.forEach(s => {
        const div = document.createElement("div");
        div.className = "satelite-card";
        div.innerHTML = `
            <strong>${s.id}</strong><br>
            Altitud: ${(s.altitud / 1000).toFixed(1)} km &nbsp;|&nbsp;
            Vel: ${Math.round(s.velocidad)} km/h<br>
            Batería: ${Math.round(s.bateria)}%
        `;
        listaSatelites.appendChild(div);
    });
}

function agregarAlerta(texto, clase) {
    const li = document.createElement("li");
    li.textContent = texto;
    li.className = clase;
    listaAlertas.prepend(li);
    while (listaAlertas.children.length > 30) {
        listaAlertas.removeChild(listaAlertas.lastChild);
    }
}

function procesarEventos(eventos) {
    (eventos || []).forEach(ev => {
        if (!idsVistos.has(ev.id)) {
            idsVistos.add(ev.id);
            agregarAlerta(ev.texto, ev.clase);
        }
    });
}

async function actualizar() {
    try {
        const [respEstado, respColisiones] = await Promise.all([
            fetch("estado_drones.json?_=" + Date.now()),
            fetch("colisiones.json?_=" + Date.now())
        ]);

        const datosEstado = respEstado.ok ? await respEstado.json() : { vehiculos: [], eventos: [] };
        const datosColisiones = respColisiones.ok ? await respColisiones.json() : { eventos: [] };

        const vehiculos = datosEstado.vehiculos || [];
        const drones = vehiculos.filter(v => v.tipo === "dron");
        const satelites = vehiculos.filter(v => v.tipo === "satelite");

        ctx.clearRect(0, 0, canvas.width, canvas.height); // solo limpia la capa de drones
        drones.forEach(dibujarDron);

        actualizarPanelSatelites(satelites);
        procesarEventos(datosEstado.eventos);
        procesarEventos(datosColisiones.eventos);
    } catch (e) {
        console.warn("Aún no hay datos disponibles", e);
    }
}

// El fondo (radar + estaciones) se dibuja UNA sola vez al cargar la página
dibujarFondoRadar();
dibujarEstaciones();

setInterval(actualizar, 1500);
actualizar();