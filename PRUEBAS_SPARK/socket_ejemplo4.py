import socket

# Configuración del servidor
HOST = 'localhost'
PORT = 9999

# Crear socket TCP
servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
servidor.bind((HOST, PORT))
servidor.listen(1)

print(f"Servidor escuchando en {HOST}:{PORT}...")
print("Escribe líneas de texto y presiona Enter para enviarlas a Spark.")
print("Ctrl+C para salir.\n")

# Aceptar conexión
conn, addr = servidor.accept()
print(f"Conectado desde {addr}\n")

try:
    while True:
        linea = input("> ")
        if linea.strip().lower() == 'salir':
            break
        conn.sendall((linea + "\n").encode('utf-8'))
except KeyboardInterrupt:
    print("\nCerrando servidor...")
finally:
    conn.close()
    servidor.close()