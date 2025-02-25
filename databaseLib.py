

import pandas as pd
from datetime import datetime
from itertools import groupby
from operator import itemgetter
import datetime
import time

class Databaselib:
        
    def __init__(self, mysql):
        self.mysql = mysql
    
    def vatimetro(self):
        try:
            cursor = self.mysql.connection.cursor()
            cursor.execute("SELECT * FROM vatimetro")
            vatimetros = cursor.fetchall()

            # Obtener nombres de columna
            column_names = [desc[0] for desc in cursor.description]
            cursor.close()

            # Convertir los resultados a una lista de diccionarios
            vatimetros_list = [dict(zip(column_names, vatimetro)) for vatimetro in vatimetros]
            
            for vatimetro in vatimetros_list:
                  
                # Verificar si 'Fecha' es de tipo datetime, y convertir a string
                if isinstance(vatimetro['Fecha'], datetime.datetime):
                    # Convertir la fecha y hora a formato string
                    fecha_str = vatimetro['Fecha'].strftime('%Y-%m-%d')
                    sec_str = vatimetro['Fecha'].strftime('%Y-%m-%d %H:%M:%S')
                    hora_str = vatimetro['Fecha'].strftime('%H:%M')
                    vatimetro['Fecha'] =  fecha_str  # Asigna la parte de la fecha
                    vatimetro['Hora'] = hora_str
                    t = time.strptime(sec_str,'%Y-%m-%d %H:%M:%S')
                    sec = int(time.mktime(t))
                    vatimetro['Sec'] = sec 
                
            return {"vatimetros": vatimetros_list}
        
        except Exception as e:
            return {"message": "Error al obtener las baterías", "error": str(e)}
        
    
    def rbmspwr(self): 
        try:
            cursor = self.mysql.connection.cursor()

            # Ejecutar la consulta para obtener los datos necesarios
            cursor.execute("""
                SELECT 
                    v.Potencia_total,
                    v.Vat_ID,
                    r.RBMS_ID,
                    r.Voltage,
                    r.Current,
                    v.Fecha
                FROM 
                    vatimetro v
                INNER JOIN 
                    rbmspwr r ON v.Fecha = r.Fecha
                WHERE 
                    r.RBMS_ID = 1
            """)

            combined_data = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]
            cursor.close()

            consolidated_data = {}
            for row in combined_data:
                row_dict = dict(zip(column_names, row))
                key = row_dict['Fecha']

                if key not in consolidated_data:
                    consolidated_data[key] = {
                        "Fecha": row_dict['Fecha'],
                        "Generada": 0,
                        "Consumida": 0,
                        "Sec": 0,
                        "Bateria": 0
                    }

                if row_dict['Vat_ID'] == 'c':
                    consolidated_data[key]['Generada'] = row_dict['Potencia_total']
                elif row_dict['Vat_ID'] == 'g':
                    consolidated_data[key]['Consumida'] = row_dict['Potencia_total']

                consolidated_data[key]['Bateria'] = row_dict['Voltage'] * row_dict['Current']

            # Convertir la fecha y calcular el valor de Sec (timestamp)
            for rbmspwr in consolidated_data.values():
                fecha_value = rbmspwr['Fecha']

                # Asegurarse de que 'Fecha' sea un string antes de convertir
                if isinstance(fecha_value, str):
                    fecha_obj = datetime.datetime.strptime(fecha_value, "%Y-%m-%d %H:%M:%S")
                elif isinstance(fecha_value, datetime.datetime):
                    fecha_obj = fecha_value
                else:
                    continue  # Si Fecha no es ni str ni datetime, saltar este registro

                # Calcular el timestamp en segundos desde el epoch
                rbmspwr['Sec'] = int(fecha_obj.timestamp())
                rbmspwr['Fecha'] = fecha_obj.strftime('%Y-%m-%d')
                rbmspwr['Hora'] = fecha_obj.strftime('%H:%M:%S')
           

            return {"rbmspwr": list(consolidated_data.values())}

        except Exception as e:
            return {"message": "Error al obtener las baterías", "error": str(e)}



    def modulos(self):
        try:
            cursor = self.mysql.connection.cursor()
            cursor.execute("SELECT * FROM rbms WHERE `ID Rack` ='RBMS1'")
            modulos = cursor.fetchall()

            # Obtener nombres de columna
            column_names = [desc[0] for desc in cursor.description]
            cursor.close()

            # Convertir los resultados a una lista de diccionarios
            modulos_list = [dict(zip(column_names, vatimetro)) for vatimetro in modulos]

            # Agrupar los registros por la fecha
            modulos_list.sort(key=itemgetter('Fecha'))

            # Asignar el ID Modulo del 1 al 12 para cada grupo de fecha
            for fecha, group in groupby(modulos_list, key=itemgetter('Fecha')):
                # Asignar ID Modulo de 1 a 12 para cada grupo con la misma fecha
                for idx, vatimetro in enumerate(group, 1):
                    vatimetro['ID Modulo'] = idx  # Asignar el número de módulo

                    # Convertir la fecha y hora a segundos desde la época Unix
                    if isinstance(vatimetro['Fecha'], datetime.datetime):
                        # Convertir la fecha y hora a formato string
                        sec_str = vatimetro['Fecha'].strftime('%Y-%m-%d %H:%M:%S')
                        t = time.strptime(sec_str, '%Y-%m-%d %H:%M:%S')
                        sec = int(time.mktime(t))
                        vatimetro['Sec'] = sec  # Asignar la fecha en segundos

            # Devolver los resultados
            return {"modulos": modulos_list}

        except Exception as e:
            return {"message": "Error al obtener los modulos", "error": str(e)}