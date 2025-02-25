import pandas as pd
from datetime import datetime
import time

class Databaselib:
        
    def __init__(self, db_connection):
        self.db_connection = db_connection

    
    def vatimetro(self):
        try:
            connection = self.db_connection()
            with connection.cursor() as cursor:  # Usar 'with' para asegurar que el cursor se cierra
                cursor.execute('SELECT * FROM vatimetro')
                vatimetros = cursor.fetchall()

                # Obtener nombres de columna
                column_names = [desc[0] for desc in cursor.description]

            # Convertir los resultados a una lista de diccionarios
            vatimetros_list = [dict(zip(column_names, vatimetro)) for vatimetro in vatimetros]
            
            for vatimetro in vatimetros_list:
                # Verificar si 'Fecha' es de tipo datetime, y convertir a string
                if isinstance(vatimetro['Fecha'], datetime):
                    # Guardar la fecha y hora original en variables separadas
                    fecha_original = vatimetro['Fecha']
                    
                    # Convertir la fecha y hora a formato string
                    vatimetro['Fecha'] = fecha_original.strftime('%Y-%m-%d')  # Parte de la fecha
                    vatimetro['Hora'] = fecha_original.strftime('%H:%M')  # Parte de la hora

                    # Convertir a segundos desde la época Unix
                    vatimetro['Sec'] = int(fecha_original.timestamp())
                
            return {"vatimetros": vatimetros_list}
        
        except Exception as e:
            return {"message": "Error al obtener las baterías", "error": str(e)}

