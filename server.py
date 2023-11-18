from aiohttp import web
import datetime
import random
import mysql.connector
import aiohttp_cors

cnx = mysql.connector.connect(user='Python', password='Python1!',
                                host='localhost',
                                database="electricity")

cursor = cnx.cursor()
get_energy_query = ("SELECT consumption, production FROM consumption "
                            "WHERE timestamp <= %s ORDER BY timestamp DESC LIMIT %s")

async def current_energy(request):
    # Извлекаем timestamp из запроса
    try:
        timestamp = request.query['timestamp']
    except KeyError:
        return web.Response(text="Timestamp is required", status=400)

    cursor.execute(get_energy_query, (timestamp, 1))

    result = cursor.fetchone()

    return web.json_response({'consumption': result[0], 'production': result[1]})

async def overall_energy(request):
    # Извлекаем timestamp из запроса
    try:
        timestamp = request.query['timestamp']
    except KeyError:
        return web.Response(text="Timestamp is required", status=400)
    
    cursor.execute(get_energy_query, (timestamp, 672))

    overall_consumption = 0
    overall_production = 0
    cnt = 0
    results = cursor.fetchall()
    if results:
        for row in results:
            consumption = row[0]
            production = row[1] 

            overall_consumption += consumption / 4
            overall_production += production / 4

    return web.json_response({'consumption': overall_consumption, 'production': overall_production})

app = web.Application()
app.router.add_get('/energy/current', current_energy)
app.router.add_get('/energy/overall', overall_energy)

cors = aiohttp_cors.setup(app, defaults={
    "*": aiohttp_cors.ResourceOptions(
            allow_credentials=True,
            expose_headers="*",
            allow_headers="*",
        )
})

# Применение политик CORS к маршрутам
for route in list(app.router.routes()):
    cors.add(route)

if __name__ == '__main__':
    web.run_app(app, port=8080)
