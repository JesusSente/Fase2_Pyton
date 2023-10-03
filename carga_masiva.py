import pandas as pd
import mysql.connector

db_config = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "",
    "database": "cooperativabd"
}

def cargar_Excel_a_Mysql(excel_file):
    try:
        df = pd.read_excel(excel_file)

        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        consulta_create_table = """
        CREATE TABLE IF NOT EXISTS tarjetas_credito (
            cod_socio INT PRIMARY KEY,
            nombre VARCHAR(50),
            apellido1 VARCHAR(50),
            apellido2 VARCHAR(50),
            num_tc VARCHAR(11),
            fch_con DATE,
            monto DECIMAL(10, 2),
            saldo DECIMAL(10, 2)
        )
        """
        cursor.execute(consulta_create_table)

        print("¡¡¡CARGANDO NUEVOS DATOS A LA BASE DE DATOS...")

        for index, row in df.iterrows():
            cod_socio = row['COD_SOCIO']
            consulta_existe = "SELECT 1 FROM tarjetas_credito WHERE cod_socio = %s"
            cursor.execute(consulta_existe, (cod_socio,))
            result = cursor.fetchone()

            if not result:
                consulta_insert = "INSERT INTO tarjetas_credito (cod_socio, nombre, apellido1, apellido2, num_tc, fch_con, monto, saldo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                cursor.execute(consulta_insert, (cod_socio, row['NOMBRE'], row['APELLIDO1'], row['APELLIDO 2'], row['#_TC'], row['FCH_CON'], row['MONTO'], row['SALDO']))

        connection.commit()
        print("¡¡¡Los datos se han cargado correctamente en la base de datos.")

    except Exception as e:
        print("***Error al cargar datos en la base de datos:", str(e))
    finally:
        cursor.close()
        connection.close()

def Consultar_Tarjetas_Credito(filtro):
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()

        consulta_select_registro = """
        SELECT * FROM tarjetas_credito
        WHERE cod_socio LIKE %s
        OR nombre LIKE %s
        OR apellido1 LIKE %s
        OR apellido2 LIKE %s
        OR num_tc LIKE %s
        """
        cursor.execute(consulta_select_registro, (filtro, filtro, filtro, filtro, filtro))

        results = cursor.fetchall()
        if results:
            for row in results:
                print("-----------------------------------")
                print("|           REGISTRO              |")
                print("-----------------------------------")
                print("Codigo Socio:", row[0])
                print("Nombre:", row[1])
                print("Apellido 1:", row[2])
                print("Apellido 2:", row[3])
                print("No. Tarjeta:", row[4])
                print("Fecha:", row[5])
                print("Monto:", row[6])
                print("Saldo:", row[7])
                print("-----------------------------------\n")
        else:
            print("***No se encontraron registros relacionados al dato ingresado")

    except Exception as e:
        print("***Error al consultar la base de datos:", str(e))
    finally:
        cursor.close()
        connection.close()


archivo_excel = 'DataPrueba.xlsx'

cargar_Excel_a_Mysql(archivo_excel)

while True:
    filtro = input("¡¡¡Ingrese el código, nombre/apellidos o número de tarjeta para buscar asociado;\n  O si desea salir ingrese (s)\n")
    
    if filtro.lower() == 's':
        print("¡¡¡Script Terminado")
        break
    else:
        Consultar_Tarjetas_Credito(filtro)
