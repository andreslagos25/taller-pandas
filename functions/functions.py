def excel_to_csv(pd, excel):
    """
        Convierte los datos de un excel a formato csv usando un archivo temporal
        para cargar los datos
        

        Params:
        PD(pandas): Recibe como argumento el objeto pandas para poder utilizar
        los metodos de la libería

        EXCEL(string): Recibe la ruta del archivo excel el cual se va a transformar
        en un archivo .csv
    """
    data = pd.read_excel(excel, index_col=None)
    
    data.to_csv("../data/temp.csv", index=False)
    return data

def change_header(pd, new_header, csv):
    """
    Cambia el encabezado de un archivo CSV por uno nuevo y elimina columnas innecesarias.
    
    Params:
    pd (pandas): El objeto pandas para utilizar sus métodos.
    new_header (list): Lista con los nuevos nombres para las columnas.
    csv (str): Ruta del archivo CSV a modificar.
    
    Returns:
    pd.DataFrame: DataFrame con el encabezado modificado.
    """
    # Leer el archivo CSV sin agregar un índice
    df = pd.read_csv(csv, encoding='latin-1', index_col=None)
    
    # Verificar si el número de columnas coincide con el nuevo encabezado
    if len(df.columns) != len(new_header):
        raise ValueError(f"El número de columnas en el DataFrame ({len(df.columns)}) "
                         f"no coincide con el número de nombres en new_header ({len(new_header)}).")

    # Asignar el nuevo encabezado
    df.columns = new_header
    # Guardar el archivo CSV con el nuevo encabezado sin índice adicional
    df.to_csv(csv, index=False)

    return df



def remove_columns(pd, csv):
    """
        Remueve las filas que no tengan fecha de modificacion
        
        Params:
        pd (pandas): El objeto pandas para utilizar sus métodos.
        csv (str): Ruta del archivo CSV a modificar.
    """
    df = pd.read_csv(csv, encoding='latin-1', index_col=None)
    df.dropna(subset=['modificado'])
    df.to_csv("../data/temp.csv")
    

def create_database(connection, nombreDB):
    """
        Funcion para crear una BD

        Parameters:
        connection (connection): Recibe la conexion con la bd
        nombreDB (str): Nombre de la base de datos
    """

    cursor = connection.cursor()
    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {nombreDB};")
        print("BD creada")
        cursor.close()
    except:
        print("La base de datos ya existe")


def create_table(connection, bd_name, table_name, table_definition):
    """
        Funcion que permite crear tablas en la bd
        Parameters:

        connection (connection): Recibe la conexion con la bd
        bd_name (str): Nombre de la base de datos
        table_name (str): Nombre de la tabla
        table_definition (str): Contiene los campos de la tabla
    """
    cursor = connection.cursor()

    try:
        # Selecciona la base de datos
        cursor.execute(f"USE {bd_name};")
        print(f"USANDO {bd_name}")

        # Verifica si la tabla ya existe
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        result = cursor.fetchone()

        if result:
            print(f"La tabla '{table_name}' ya existe.")
        else:
            # Crea la nueva tabla
            cursor.execute(f"CREATE TABLE {table_name} ({table_definition});")
            connection.commit()  # Asegúrate de hacer commit si hay cambios en la DB
            print("Tabla creada correctamente.")
    except Exception as e:
        print(f"Error al crear la tabla: {e}")
    finally:
        cursor.close()  # Cierra el cursor al final

def load_data(connection, bd_name):
    """
        Carga un archivo .csv a la la bd, en la tabla producto

        PARAMETERS:
        connection (connection): Recibe la conexion a la BD
        name_bd (str): Recibe el nombre de la bd
    """
    cursor = connection.cursor()
    try:
        cursor.execute(f"USE {bd_name};")
        print(f"USANDO {bd_name}")

        str = """
                LOAD DATA INFILE '/var/lib/mysql-files/data.csv'
                IGNORE INTO TABLE producto
                FIELDS TERMINATED BY ','
                ENCLOSED BY '"'
                LINES TERMINATED BY '\n'
                IGNORE 1 LINES(@ID,@STOCK,@MATERIAL,@DESCRIPCION,@TIPOMATERIAL,@umb,@mat,@sm,@GRUPO,@JERARQUIA,@NUMEROFAB,@FABRICANTE,@EXTENSION,@CREADO,@MODIFICADO)
                SET material = @MATERIAL, descripcion = @DESCRIPCION, fabricante = @FABRICANTE, modificado = @MODIFICADO;
            """
    except Exception as e:
         print(f"Error al crear la tabla: {e}")
    finally:
        cursor.close()  # Cierra el cursor al final


