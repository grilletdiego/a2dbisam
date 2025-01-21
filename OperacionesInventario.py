from flask import Blueprint, jsonify, request, current_app, Response
from bson.json_util import dumps
import struct
from datetime import datetime
# Crea un Blueprint para este módulo
operaciones_bp = Blueprint('operaciones', __name__, url_prefix='/api/operaciones')


@operaciones_bp.route('/libroventas', methods=['GET'])
def libro_ventas():
    serie = request.args.get('serie')
    fecha_inicial = request.args.get('fecha_inicial')
    fecha_final = request.args.get('fecha_final')

    # Lista de todas las posibles series
    all_series = ['HKA01', 'HKA02', 'HKA03']

    # Iniciar la cláusula WHERE
    where_clause = f"""
    FTI_FECHAEMISION BETWEEN '{fecha_inicial}' 
    AND '{fecha_final}' 
    AND (FTI_TIPO = 11 OR FTI_TIPO = 12) 
    AND FTI_STATUS = 1 
    """

    # Añadir condición de serie
    if serie == 'TODAS':
        series_str = "', '".join(all_series)  # Crear un string para la cláusula SQL IN
        where_clause = f"FTI_SERIE IN ('{series_str}') AND " + where_clause
    else:
        where_clause = f"FTI_SERIE = '{serie}' AND " + where_clause

    sql_query = f"""
        SELECT 
        FTI_FECHAEMISION, FTI_RIFCLIENTE, FTI_PERSONACONTACTO, 
        FTI_TIPO, FTI_DOCUMENTO, FTI_DOCUMENTOORIGEN, 
        FTI_MAQUINAFISCAL, FTI_TOTALNETO, FTI_BASEIMPONIBLE, 
        FTI_IMPUESTO1PORCENT, FTI_IMPUESTO1MONTO,FTI_BASEIGTF, FTI_SERIE
        FROM SOperacionInv
        WHERE 
            {where_clause}
            ORDER BY FTI_FECHAEMISION ASC, FTI_SERIE ASC, FTI_TIPO ASC, FTI_DOCUMENTO ASC
        """
    odbc_rows = fetch_odbc_data(sql_query)

    # Crear un diccionario para cada fila explícitamente
    rows_as_dict = []
    for row in odbc_rows:
        try:
            maquina_fiscal, z = row.FTI_MAQUINAFISCAL.split('\t')
        except ValueError:
            maquina_fiscal = row.FTI_MAQUINAFISCAL  
            z = None  

        if z is not None:
            z = str(int(z)).zfill(4)
   
        fecha = row.FTI_FECHAEMISION.strftime('%d/%m/%Y')
        tipo = 'FAC' if row.FTI_TIPO == 11 else 'DEV' if row.FTI_TIPO == 12 else row.FTI_TIPO
        documento_origen = row.FTI_DOCUMENTOORIGEN if tipo == 'DEV' else ''
        inversor = -1 if tipo == 'DEV' else 1
        total_neto = row.FTI_TOTALNETO*inversor
        base_imponible = row.FTI_BASEIMPONIBLE*inversor
        impuesto = row.FTI_IMPUESTO1MONTO*inversor
        igtf = row.FTI_BASEIGTF*inversor
     
        es_contribuyente = row.FTI_RIFCLIENTE.startswith(('J', 'G')) if row.FTI_RIFCLIENTE else False

        row_dict = {
            'FTI_FECHAEMISION': fecha,
            'FTI_RIFCLIENTE': row.FTI_RIFCLIENTE,
            'FTI_PERSONACONTACTO': row.FTI_PERSONACONTACTO,
            'FTI_TIPO': tipo,
            'FTI_DOCUMENTO': row.FTI_DOCUMENTO,
            'FTI_DOCUMENTOORIGEN': documento_origen,
            'FTI_MAQUINAFISCAL': maquina_fiscal,
            'Z': z,
            'FTI_TOTALNETO': total_neto,
            'FTI_excento': 0,
            'base_no_contribuyentes': None if es_contribuyente else base_imponible,
            'alicouta_no_contribuyentes': None if es_contribuyente else row.FTI_IMPUESTO1PORCENT,
            'monto_no_contribuyentes': None if es_contribuyente else impuesto,
            'base_contribuyentes': base_imponible if es_contribuyente else None,
            'alicouta_contribuyentes': row.FTI_IMPUESTO1PORCENT if es_contribuyente else None,
            'monto_contribuyentes': impuesto if es_contribuyente else None,
            'BASE_IGTF': igtf,
            'IGTF': igtf * 0.03,
        }
        rows_as_dict.append(row_dict)  
            

    return Response(dumps(rows_as_dict), mimetype='application/json')

@operaciones_bp.route('/librocompras', methods=['GET'])
def libro_compras():
    
    fecha_inicial = request.args.get('fecha_inicial')
    fecha_final = request.args.get('fecha_final')

    sql_query = f"""
    SELECT 
    so.FTI_FECHAEMISION, so.FTI_FECHALIBRO, so.FTI_RIFCLIENTE, so.FTI_PERSONACONTACTO,
    so.FTI_TIPO, so.FTI_DOCUMENTO, so.FTI_DOCUMENTOORIGEN,
    so.FTI_NUMEROCONTROL, so.FTI_TOTALNETO, so.FTI_BASEIMPONIBLE,
    so.FTI_IMPUESTO1PORCENT, so.FTI_IMPUESTO1MONTO, 
    sr.FCP_NUMERO as retencion
    FROM SOperacionInv so
    LEFT JOIN SretencionProveedor sr
    ON so.FTI_RESPONSABLE = sr.FCP_CODIGO 
    AND so.FTI_DOCUMENTO = sr.FCP_NUMERO2
    WHERE 
    so.FTI_FECHALIBRO BETWEEN '{fecha_inicial}' AND '{fecha_final}'
    AND so.FTI_TIPO IN (6, 7)
    AND so.FTI_STATUS = 1
    ORDER BY so.FTI_FECHAEMISION ASC, so.FTI_TIPO ASC, so.FTI_DOCUMENTO ASC;
    """

    odbc_rows = fetch_odbc_data(sql_query)

    # Crear un diccionario para cada fila explícitamente
    rows_as_dict = []
    for row in odbc_rows:
        fecha = row.FTI_FECHAEMISION.strftime('%d/%m/%Y')
        numero_retencion = f"{row.FTI_FECHALIBRO.strftime("%Y%m")}{row.retencion}"
            
        tipo = 'FAC' if row.FTI_TIPO == 6 else 'DEV' if row.FTI_TIPO == 7 else row.FTI_TIPO
        documento_origen = row.FTI_DOCUMENTOORIGEN if tipo == 'DEV' else ''
        inversor = -1 if tipo == 'DEV' else 1
        total_neto = row.FTI_TOTALNETO*inversor
        base_imponible = row.FTI_BASEIMPONIBLE*inversor
        impuesto = row.FTI_IMPUESTO1MONTO*inversor
        
        row_dict = {
                'FTI_FECHAEMISION': fecha,
                'FTI_RIFCLIENTE': row.FTI_RIFCLIENTE,
                'FTI_PERSONACONTACTO': row.FTI_PERSONACONTACTO,
                'FTI_TIPO': tipo,
                'FTI_DOCUMENTO': row.FTI_DOCUMENTO,
                'FTI_DOCUMENTOORIGEN': documento_origen,
                'FTI_NUMEROCONTROL' : row.FTI_NUMEROCONTROL,
                'FTI_TOTALNETO': total_neto,
                'FTI_excento': total_neto - base_imponible - impuesto,
                'FTI_BASEIMPONIBLE': base_imponible,
                'FTI_IMPUESTO1PORCENT': row.FTI_IMPUESTO1PORCENT,
                'FTI_IMPUESTO1MONTO': impuesto,
                'numero_retencion' : numero_retencion,
                'retenido' : impuesto*0.75
            }
        rows_as_dict.append(row_dict) 

    return Response(dumps(rows_as_dict), mimetype='application/json')

def decode_record(blob_data):
    record_format = 'i 40s 69s ? 4s B 5s d ? d q ? 7x' #40s 69s puede modificarse pero no sumar mas de 109
    record_size = struct.calcsize(record_format)

    num_records = len(blob_data) // record_size
    records = []
    tipo_pago_totals = {}
    
    # Mapeo de tipos de pago
    tipo_pago_map = {
        0: 'efectivo',
        1: 'pago_movil',
        2: 'tarjeta_debito',
        3: 'bio_pago'
    }
    
    for i in range(num_records):
        start = i * record_size
        end = start + record_size
        record_data = blob_data[start:end]
        
        values = struct.unpack(record_format, record_data)
        
        # Renombrar TipoPago según el mapeo
        tipo_pago = tipo_pago_map.get(values[0], 'otro')
        monto_pago = values[10]/10000
        BaseRetencion = values[7]  
        
        #record = {
        #    'TipoPago': values[0],
        #    'BancoTarjeta': values[1].decode('ascii', errors='ignore').lstrip('\x02').strip('\x00'), #devevuelve el code de Sinstitucion
        #    'Detalle': values[2].decode('ascii', errors='ignore').replace('\x04', '').strip('\x00'),
        #    'DepositoDone': values[3],
        #    'CodeMoneda': int.from_bytes(values[4], byteorder='big') & 0xFF  ,  #devevuelve el code de Smoneda
        #    'OrigenPago': values[5],
        #    'CodeAutoRetencion' :values[6].decode('ascii', errors='ignore').strip('\x00'),
        #    'BaseRetencion': values[7],
        #    'RetencionIVA': values[8],
        #    'MontoBsViejos': values[9],
        #    'MontoPago': values[10] / 10000,  # Ajuste para los 4 decimales
        #    'NoEsEfectivo' : values[10]
        #}

        #records.append(record)
        
        # Si el factor es mayor que 0, se considera divisa
        if BaseRetencion > 0 :
            tipo_pago = 'divisa'
            tipo_pago_totals['factor'] = BaseRetencion

        # Sumar MontoPago por TipoPago
        if tipo_pago in tipo_pago_totals:
            tipo_pago_totals[tipo_pago] += monto_pago
        else:
            tipo_pago_totals[tipo_pago] = monto_pago
        
    #tipo_pago_totals['values'] = records  
          
    return tipo_pago_totals

    
    

@operaciones_bp.route('/ventasdiarias', methods=['GET'])
def ventasdiarias():
    fecha_inicial = request.args.get('fecha_inicial')
    sql_query = f"""
        SELECT 
        SOperacionInv.FTI_FECHAEMISION, 
        SOperacionInv.FTI_SERIE, 
        SOperacionInv.FTI_DOCUMENTO, 
        SOperacionInv.FTI_TIPO, 
        SOperacionInv.FTI_TOTALNETO, 
        SOperacionInv.FTI_FORMADEPAGO, 
        SOperacionInv.FTI_VUELTO, 
        SOperacionInv.FTI_BASEIGTF, 
        SOperacionInv.FTI_MACHINENAME, 
        SOperacionInv.FTI_DESCRIPCLASIFY, 
        SOperacionInv.FTI_HORA, 
        SOperacionInv.FTI_FACTORREFERENCIA,
        Svendedores.FV_DESCRIPCION AS VENDEDOR
        FROM SOperacionInv
        LEFT JOIN Svendedores 
        ON SOperacionInv.FTI_VENDEDORASIGNADO = Svendedores.FV_CODIGO
        WHERE 
            SOperacionInv.FTI_FECHAEMISION = '{fecha_inicial}'
            AND SOperacionInv.FTI_TIPO IN (11, 12)
            AND SOperacionInv.FTI_STATUS = 1
        ORDER BY 
            SOperacionInv.FTI_TIPO ASC, 
            SOperacionInv.FTI_SERIE ASC, 
            SOperacionInv.FTI_HORA ASC
        """

    odbc_rows = fetch_odbc_data(sql_query)

    rows_as_dict = []
    factor_referencia = None

    for row in odbc_rows:
        tipo = 'Facturas' if row.FTI_TIPO == 11 else 'Devoluciones' if row.FTI_TIPO == 12 else row.FTI_TIPO
        inversor = -1 if tipo == 'Devoluciones' else 1
        total_neto = row.FTI_TOTALNETO * inversor

        # Decodificar formas de pago
        forma_de_pago_totals = decode_record(row.FTI_FORMADEPAGO)

        divisa = forma_de_pago_totals.get('divisa', 0) 
        total_divisa = 0
        if divisa > 0:
            base_igtf = row.FTI_BASEIGTF
            igtf = 0 if row.FTI_MACHINENAME == "CAJA01" else base_igtf * 0.03   # Condición solo válida para Distormonca
            total_divisa = (divisa + igtf) * inversor

        row_dict = {
            'serie': row.FTI_SERIE,
            'numero': row.FTI_DOCUMENTO,
            'tipo': tipo, 
            'total': total_neto,
            'efectivo': forma_de_pago_totals.get('efectivo', 0) * inversor,
            'divisa': total_divisa,
            'tarjeta_debito': forma_de_pago_totals.get('tarjeta_debito', 0) * inversor,
            'bio_pago': forma_de_pago_totals.get('bio_pago', 0) * inversor,
            'pago_movil': forma_de_pago_totals.get('pago_movil', 0) * inversor,
            'vuelto': row.FTI_VUELTO * inversor,
            'hora': row.FTI_HORA.strftime('%H:%M:%S'),
            'maquina': row.FTI_MACHINENAME,
            'vendedor': row.VENDEDOR
        }
        rows_as_dict.append(row_dict)

        # Asignar el valor de FTI_FACTORREFERENCIA si aún no se ha asignado
        if factor_referencia is None:
            factor_referencia = row.FTI_FACTORREFERENCIA

    # Preparar la respuesta en formato JSON
    response_data = {
        'data': rows_as_dict,
        'factor': factor_referencia
    }

    return Response(dumps(response_data), mimetype='application/json')

@operaciones_bp.route('/detalleventa', methods=['GET'])
def detalleventa():
    serie = request.args.get('serie')
    factura = request.args.get('factura').zfill(8)

    sql_query = f"""
        SELECT SD.FDI_CODIGO,SI.FI_DESCRIPCION,SD.FDI_CANTIDAD   
            FROM 
                SOPERACIONINV SO
            JOIN 
                SDETALLEVENTA SD 
            ON 
                SO.FTI_DOCUMENTO = SD.FDI_DOCUMENTO
                AND SO.FTI_FECHAEMISION = SD.FDI_FECHAOPERACION
            JOIN 
                SINVENTARIO SI 
            ON 
                SD.FDI_CODIGO = SI.FI_CODIGO
            WHERE 
                SO.FTI_SERIE = '{serie}' AND
                SO.FTI_DOCUMENTO = '{factura}' 
        """
    odbc_rows = fetch_odbc_data(sql_query)

    rows_as_dict = []
    for row in odbc_rows:
        row_dict = {
            'codigo': row.FDI_CODIGO,
            'descripcion' : row.FI_DESCRIPCION,
            'cantidad' : row.FDI_CANTIDAD
            }
        rows_as_dict.append(row_dict)
            

    return Response(dumps(rows_as_dict), mimetype='application/json')


@operaciones_bp.route('/retencion', methods=['GET'])
def retencion():
    serie = request.args.get('serie')
    factura = request.args.get('factura').zfill(8)

    sql_query = f"""
        SELECT * FROM SOperacionInv
        WHERE FTI_DOCUMENTO = '{factura}' AND FTI_SERIE = '{serie}'
        """

    odbc_rows = fetch_odbc_data(sql_query)

    # Crear un diccionario para cada fila explícitamente
    rows_as_dict = []
    for row in odbc_rows:
        row_dict = {
            'fecha_emision': row.FTI_FECHAEMISION.strftime('%d/%m/%Y'),
            'rif_cliente': row.FTI_RIFCLIENTE,
            'cliente': row.FTI_PERSONACONTACTO,
            'total_neto': row.FTI_TOTALNETO,
            'base': row.FTI_BASEIMPONIBLE,
            'iva': row.FTI_IMPUESTO1MONTO,
            'monto_retencion': row.FTI_IMPUESTO1MONTO * 0.75
            }
        rows_as_dict.append(row_dict)
            

    return Response(dumps(rows_as_dict), mimetype='application/json')

@operaciones_bp.route('/update_factor', methods=['PUT'])
def update_factor():
    try:
        fm_factor = request.args.get('fm_factor', type=float)  # Extrae el valor del parámetro 'fm_factor'

        if fm_factor is None:
            return jsonify({'error': 'fm_factor debe ser un número válido'}), 400
        

        # Obtener la conexión ODBC
        odbc_manager = current_app.odbc_manager
        connection = odbc_manager.get_connection()
        cursor = connection.cursor()

        # Obtener la fecha actual en formato 'YYYY-MM-DD'
        hoy = datetime.now().strftime('%Y-%m-%d')

        # Consulta SQL para actualizar los valores
        query = f"""
        UPDATE Smoneda
        SET
            FM_LASTFACTOR = FM_FACTOR,
            FM_FACTOR = {fm_factor},
            FM_LASTUPDATE = '{hoy}'
        WHERE FM_CODE = 2
        """
        
        cursor.execute(query)
        connection.commit()

        return jsonify({'success': f'Se actualizó FACTOR a {fm_factor}'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500




def fetch_odbc_data(query):
    odbc_manager = current_app.odbc_manager
    connection = odbc_manager.get_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    odbc_rows = cursor.fetchall()
    return odbc_rows
