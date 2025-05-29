from fastapi import FastAPI
import psycopg2
import redis
 
redis_client = redis.StrictRedis(host='10.10.8.4', port=6379, db=0)
 
def get_connection():
    try:
        connection = psycopg2.connect(
            dbname="viajes_sanabria",
            user="admin_sanabria",
            password="admin123",
            host="10.10.9.4",
            port="5432"
        )
        return connection
    except Exception as e:
        print(f"Error: {e}")
       
app = FastAPI()
 
@app.get("/health/postgres")
def health_check():
    conn = get_connection()
    if conn:
        conn.close()
        return {"status": "PostgreSQL OK"}
    else:
        return {"status": "Error de conexión con PostgreSQL"}
 
def get_redis_connection():
    try:
        r = redis.Redis(host="10.10.8.4", port=6379, db=0)
        r.ping()  # Verifica si Redis está activo
        return r
    except redis.ConnectionError as e:
        print(f"Error al conectar a Redis: {e}")
        return None
 
@app.get("/health/redis")
def health_check_redis():
    redis_client = get_redis_connection()
    if redis_client:
        return {"status": "Redis OK"}
    else:
        return {"status": "Error: Redis no disponible"}
    
    
   
@app.get("/trips/total")
def count():
    # intentar obtener dato desde redis
    t3 = redis_client.get("trips:total")
    if t3:
        return {"total_trips":t3, "source": "redis"}
   
    # obtener datos desde postgres
    conn = get_connection()
    cursor = conn.cursor()
    print(cursor.execute("SELECT COUNT(*) FROM viajes;"))
    t3=cursor.fetchone()[0]
    cursor.close()
    conn.close()
    redis_client.setex("trips:total", 60, t3)
    return {"total_trips":t3, "source": "postgres"}

@app.get("/trips/total/localities")
def trips_by_locality():
    # Intentar obtener desde Redis
    result = redis_client.get("trips:by_locality")
    if result:
        return {"localities": eval(result.decode()), "source": "redis"}

    # Obtener desde PostgreSQL
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT l.nombre, COUNT(*)
        FROM viajes v
        JOIN estaciones e ON v.estacion_abordaje_id = e.estacion_id
        JOIN localidades l ON e.localidad_id = l.localidad_id
        GROUP BY l.nombre;
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    localities_dict = {row[0]: row[1] for row in rows}
    redis_client.setex("trips:by_locality", 60, str(localities_dict))
    return {"localities": localities_dict, "source": "postgres"}

@app.get("/finance/revenue")
def total_revenue():
    # Verificar si ya está en Redis
    result = redis_client.get("finance:revenue")
    if result:
        data = eval(result.decode())
        data["source"] = "redis"
        return data

    # Consultar desde PostgreSQL
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT SUM(t.valor)
        FROM viajes v
        JOIN tarifas t ON v.tarifa_id = t.tarifa_id;
    """)

    total = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    revenue_data = {
        "total_revenue": float(total or 0),  # Asegura que sea 0 si no hay datos
        "currency": "COP",
        "source": "postgres"
    }
    # Guardar en Redis sin el campo "source"
    redis_cache = {
        "total_revenue": revenue_data["total_revenue"],
        "currency": revenue_data["currency"]
    }

    redis_client.setex("finance:revenue", 60, str(revenue_data))
    return revenue_data

@app.get("/finance/revenue/localities")
def revenue_by_locality():
    # Intentar obtener desde Redis
    result = redis_client.get("finance:revenue:localities")
    if result:
        data = eval(result.decode())
        data["source"] = "redis"
        return data

    # Conectar a PostgreSQL
    conn = get_connection()
    cursor = conn.cursor()

    # Consulta SQL para calcular ingresos por localidad
    cursor.execute("""
        SELECT l.nombre AS localidad, SUM(t.valor) AS total
        FROM viajes v
        JOIN tarifas t ON v.tarifa_id = t.tarifa_id
        JOIN estaciones e ON v.estacion_abordaje_id = e.estacion_id
        JOIN localidades l ON e.localidad_id = l.localidad_id
        GROUP BY l.nombre;
    """)

    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    revenue_map = {row[0]: float(row[1]) for row in rows}

    response = {
        "revenue_by_locality": revenue_map,
        "currency": "COP",
        "source": "postgres"
    }

    # Guardar en Redis sin "source"
    redis_cache = {
        "revenue_by_locality": revenue_map,
        "currency": "COP"
    }
    redis_client.setex("finance:revenue:localities", 60, str(redis_cache))

    return response
