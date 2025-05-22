from fastapi import FastAPI
import psycopg2

def get_connection():
    try:
        connection = psycopg2.connect(
            dbname="sistema_recargas_viajes",
            user="admin",
            password="Pass!__2025!",
            host="149.130.169.172",
            port="33333"
        )
        return connection
    except Exception as e:
        print(f"Error: {e}")

app = FastAPI()

@app.get("/users/count")
def count():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM usuarios;")  
    total = cursor.fetchone()[0]  
    cursor.close()
    conn.close()
    return {"total_users": total}

@app.get("/users/active/count")
def count():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT u.usuario_id) FROM usuarios u INNER JOIN tarjetas t ON u.usuario_id = t.usuario_id WHERE t.estado = 'Activa'") 
    total = cursor.fetchone()[0]  
    cursor.close()
    conn.close()
    return {"total_users": total}

@app.get("/users/latest")
def count():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT u.usuario_id, u.nombre, u.apellido FROM usuarios u ORDER BY usuario_id DESC LIMIT 1 ") 
    result = cursor.fetchone()  
    cursor.close()
    conn.close()
    user_id, nombre, apellido = result
    full_name = f"{nombre} {apellido}"
    return {"latest_user":{
        "id": user_id,
        "full_name": full_name}
    }   

@app.get("/trips/total")
def count():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM viajes;") 
    total = cursor.fetchone()[0]  
    cursor.close()
    conn.close()
    return {"total_trips": total}  

@app.get("/finance/revenue")
def count():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(t.valor) AS total_revenue FROM viajes v JOIN tarifas t ON v.tarifa_id = t.tarifa_id;") 
    total = cursor.fetchone()[0]  
    cursor.close()
    conn.close()
    return {"total_revenue": float(total), 
            "currency": "COP"} 