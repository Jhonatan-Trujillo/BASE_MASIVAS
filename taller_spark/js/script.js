const canvas = document.getElementById("mapa");
const ctx = canvas.getContext("2d");
const canvasFondo = document.getElementById("mapa-fondo");
const ctxFondo = canvasFondo.getContext("2d");

const listaAlertas = document.getElementById("lista-alertas");
const listaSatelites = document.getElementById("lista-satelites");
const indicadorEstado = document.getElementById("estado-conexion");

const LAT_MIN = 4.40, LAT_MAX = 4.46;
const LON_MIN = -75.26, LON_MAX = -75.20;

const DURACION_INTERPOLACION_MS = 2000;  // tiempo de desplazamiento suave entre dos posiciones

const idsVistos = new Set();
const historialPosiciones = {};  // id -> [{x,y}, ...] para la estela
const estadoVehiculos = {};      // id -> estado de interpolación de cada dron
const tarjetasSatelites = {};    // id -> elemento HTML (se crea una sola vez)

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

function lerp(a, b, t) {
    return a + (b - a) * t;
}

// Interpola el ángulo por el camino más corto (evita giros "al revés" al cruzar 0°/360°)
function lerpAngulo(a, b, t) {
    const diferencia = ((b - a + 540) % 360) - 180;
    return (a + diferencia * t + 360) % 360;
}

// --- Capa de FONDO: se dibuja una sola vez ---

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

// --- Capa DINÁMICA (drones + estelas) ---

function dibujarEstela(id, x, y) {
    if (!historialPosiciones[id]) historialPosiciones[id] = [];
    const puntos = historialPosiciones[id];
    puntos.push({ x, y });
    if (puntos.length > 40) puntos.shift();

    ctx.beginPath();
    puntos.forEach((p, i) => {
        ctx.globalAlpha = (i / puntos.length) * 0.4;
        if (i === 0) ctx.moveTo(p.x, p.y);
        else ctx.lineTo(p.x, p.y);
    });
    ctx.strokeStyle = "#38bdf8";
    ctx.lineWidth = 2;
    ctx.stroke();
    ctx.globalAlpha = 1;
}

function dibujarDron(v, x, y, rumbo) {
    dibujarEstela(v.id, x, y);

    const tieneAlerta = v.alerta_bateria || v.alerta_altitud || v.alerta_velocidad;

    ctx.save();
    ctx.translate(x, y);
    ctx.rotate(rumbo * Math.PI / 180);
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

// --- Panel de satélites: las tarjetas se crean UNA vez y luego solo se actualizan sus textos ---

function crearTarjetaSatelite(id) {
    const tarjeta = document.createElement("div");
    tarjeta.className = "satelite-card";
    tarjeta.innerHTML = `
        <strong>${id}</strong><br>
        Altitud: <span data-campo="altitud"></span> km &nbsp;|&nbsp;
        Vel: <span data-campo="velocidad"></span> km/h<br>
        Batería: <span data-campo="bateria"></span>%
    `;
    listaSatelites.appendChild(tarjeta);
    return tarjeta;
}

function actualizarPanelSatelites(satelites) {
    satelites.forEach(s => {
        if (!tarjetasSatelites[s.id]) {
            tarjetasSatelites[s.id] = crearTarjetaSatelite(s.id);
        }
        const tarjeta = tarjetasSatelites[s.id];
        tarjeta.querySelector('[data-campo="altitud"]').textContent = (s.altitud / 1000).toFixed(1);
        tarjeta.querySelector('[data-campo="velocidad"]').textContent = Math.round(s.velocidad);
        tarjeta.querySelector('[data-campo="bateria"]').textContent = Math.round(s.bateria);
    });
}

// --- Panel de alertas: solo se AGREGAN elementos nuevos arriba, nunca se reconstruye la lista ---

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

// --- Recepción de datos: el servidor EMPUJA cada cambio (Server-Sent Events) ---

function recibirDatos(datos) {
    const vehiculos = datos.vehiculos || [];
    const ahora = performance.now();

    vehiculos.filter(v => v.tipo === "dron").forEach(v => {
        const { x, y } = proyectar(v.lat, v.lon);
        const previo = estadoVehiculos[v.id];

        estadoVehiculos[v.id] = {
            datos: v,
            prevX: previo ? previo.xActual(ahora) : x,
            prevY: previo ? previo.yActual(ahora) : y,
            prevRumbo: previo ? previo.rumboActual(ahora) : (v.rumbo || 0),
            targetX: x,
            targetY: y,
            targetRumbo: v.rumbo || 0,
            inicio: ahora,
            xActual(t) { return lerp(this.prevX, this.targetX, this._t(t)); },
            yActual(t) { return lerp(this.prevY, this.targetY, this._t(t)); },
            rumboActual(t) { return lerpAngulo(this.prevRumbo, this.targetRumbo, this._t(t)); },
            _t(t) { return Math.min(1, (t - this.inicio) / DURACION_INTERPOLACION_MS); }
        };
    });

    actualizarPanelSatelites(vehiculos.filter(v => v.tipo === "satelite"));
    procesarEventos(datos.eventos);
}

// --- Indicador de conexión (arriba, junto al título) ---

function mostrarEstado(texto, clase) {
    indicadorEstado.textContent = texto;
    indicadorEstado.className = clase;
}

function conectarStream() {
    mostrarEstado("● Conectando con el servidor...", "estado-espera");
    const fuente = new EventSource("/api/stream");

    fuente.onopen = () => {
        mostrarEstado("● Conectado al servidor, esperando datos del consumer...", "estado-espera");
    };

    fuente.onmessage = (mensaje) => {
        try {
            const datos = JSON.parse(mensaje.data);
            recibirDatos(datos);
            mostrarEstado(`● En vivo (${(datos.vehiculos || []).length} vehículos)`, "estado-vivo");
        } catch (e) {
            console.warn("Mensaje del stream no válido", e);
        }
    };

    fuente.onerror = () => {
        mostrarEstado("● Sin conexión con el servidor (¿api.py está corriendo? ¿abriste localhost:5000?)", "estado-error");
    };
}

// --- Renderizado: corre en cada fotograma, desacoplado de la llegada de datos ---

function renderizar() {
    const ahora = performance.now();

    ctx.clearRect(0, 0, canvas.width, canvas.height);
    Object.values(estadoVehiculos).forEach(estado => {
        dibujarDron(
            estado.datos,
            estado.xActual(ahora),
            estado.yActual(ahora),
            estado.rumboActual(ahora)
        );
    });

    requestAnimationFrame(renderizar);
}

dibujarFondoRadar();
dibujarEstaciones();
conectarStream();
requestAnimationFrame(renderizar);