# -*- coding: utf-8 -*-
"""
Created on Tue Nov 21 09:53:04 2023

@author: manuel.manodes
"""
import pandas as pd
import itertools
from datetime import datetime
import json

# Obtención de fecha y hora actual
fecha_actual = datetime.now().strftime("%Y-%m-%d")
hora_actual = datetime.now().strftime("%H%M%S")

# Leemos los archivos de fuente de entrada, así mismo debemos validar que los encabezados sean correctos y limpiar de espacios.
ruta_lista_distribucion = "C:/Users/manuel.manodes/Documents/Desarrollos/Local/Transbank/Auto/Entradas/Lista de distribución.csv"
ruta_tabla_iata = "C:/Users/manuel.manodes/Documents/Desarrollos/Local/Transbank/Auto/Anexo/Tabla IATA.csv"
ruta_anexo_sku = "C:/Users/manuel.manodes/Documents/Desarrollos/Local/Transbank/Auto/Anexo/Anexo sku.csv"
tabla_iata = pd.read_csv(ruta_tabla_iata, delimiter=';', encoding='latin-1')
lista_distribucion = pd.read_csv(ruta_lista_distribucion, delimiter=';', encoding='latin-1')
anexo_sku = pd.read_csv(ruta_anexo_sku, delimiter=';', encoding='latin-1')
lista_distribucion = lista_distribucion.rename(columns=lambda x: x.strip())

# Encabezados esperados
encabezados_esperados =['ID_PEDIDO','NUM_LISTA','ORDEN_DIST','TIPO_ORDEN','COD_COMERCIO','NOM_COMERCIO','DIR_COMERCIO',
                        'NRO_CALLE','NRO_LOCAL','COD_COMUNA','COMUNA','COD_CIUDAD','CIUDAD','REGION','TIPO_INSUMO','FECHA_LISTA',
                        'Q_ENTREGA','NUM_SOLICITUD_SS','OBS_REPARTO','CONTACTO','ESTADO','MOTIVO_NOENTREGA','FECHA_RENDICION',
                        'HORA24_ENTREGA','RECEPTOR','OBS_RENDICION']

# Comparar encabezados
if list(lista_distribucion.columns) != encabezados_esperados:
    # Identificar encabezados que no coinciden
    encabezados_erroneos = set(lista_distribucion.columns) ^ set(encabezados_esperados)

    # Mostrar mensaje de error
    print(f"Error: Los encabezados no coinciden. Encabezados incorrectos: {encabezados_erroneos}")
else:
    
# Luego de validar el correcto registro de los encabezados, procedemos a limpiar el archivo.
    lista_distribucion['ID_PEDIDO'] = lista_distribucion['ID_PEDIDO'].str.replace(',', '')
    lista_distribucion['MIN'] = lista_distribucion['CONTACTO'].apply(lambda x: len(list(itertools.takewhile(lambda c: not c.isdigit(), x))))
    lista_distribucion['FONO'] = lista_distribucion['CONTACTO'].str.extract('(\d+)')
    lista_distribucion['FONO'].fillna('0', inplace=True)
    lista_distribucion['FONO'] = lista_distribucion['FONO'].astype('int64')
    lista_distribucion['PESO'] = lista_distribucion['Q_ENTREGA']*60
    lista_distribucion = pd.merge(lista_distribucion, tabla_iata, on='COMUNA', how='left')
    lista_distribucion['NRO_LOCAL'].fillna('0', inplace=True)
    lista_distribucion['FECHA_LISTA'] = pd.to_datetime(lista_distribucion['FECHA_LISTA'], errors='coerce')
    lista_distribucion['FECHA_LISTA'] = lista_distribucion['FECHA_LISTA'].dt.strftime("%Y-%m-%d")

lista_distribucion = lista_distribucion[['ID_PEDIDO','NUM_LISTA','ORDEN_DIST','TIPO_ORDEN','COD_COMERCIO','NOM_COMERCIO','DIR_COMERCIO',
                        'NRO_CALLE','NRO_LOCAL','COD_COMUNA','COMUNA','COD_CIUDAD','CIUDAD','REGION','TIPO_INSUMO','FECHA_LISTA',
                        'Q_ENTREGA','NUM_SOLICITUD_SS','OBS_REPARTO','CONTACTO','ESTADO','MOTIVO_NOENTREGA','FECHA_RENDICION',
                        'HORA24_ENTREGA','RECEPTOR','OBS_RENDICION','MIN','FONO','PESO','BASE']]

# Creación de sku
lista_distribucion['Anexo'] = lista_distribucion['TIPO_INSUMO'].apply(lambda x: x.replace(" ", ""))
lista_distribucion = pd.merge(lista_distribucion, anexo_sku, on='Anexo', how='inner')

carga_blue_API = lista_distribucion

# Cambio de encabezados para encajar en el formato de API
carga_blue_API = carga_blue_API.rename(columns={'ID_PEDIDO':'order_nbr_id'}) 
carga_blue_API = carga_blue_API.rename(columns={'Sku':'item_nbr'}) 
carga_blue_API = carga_blue_API.rename(columns={'Descripción':'item_desc'}) 
carga_blue_API = carga_blue_API.rename(columns={'Q_ENTREGA':'ordered_qty'}) 
carga_blue_API = carga_blue_API.rename(columns={'TIPO_ORDEN':'doctype'})
carga_blue_API = carga_blue_API.rename(columns={'REGION':'region'})
carga_blue_API = carga_blue_API.rename(columns={'COD_COMUNA':'sector'})
carga_blue_API = carga_blue_API.rename(columns={'COD_CIUDAD':'locality'})
carga_blue_API = carga_blue_API.rename(columns={'DIR_COMERCIO':'address'})
carga_blue_API = carga_blue_API.rename(columns={'NRO_CALLE':'street_number'})
carga_blue_API = carga_blue_API.rename(columns={'NRO_LOCAL':'floor_number'})
carga_blue_API = carga_blue_API.rename(columns={'FONO':'cellphone'})
carga_blue_API = carga_blue_API.rename(columns={'FECHA_LISTA':'ordered_date'})

# Crear campos variables con fecha
carga_blue_API['shipping_date'] = fecha_actual

# Crear campos replicados para encajar en el formato API 
carga_blue_API['order_nbr'] = carga_blue_API['order_nbr_id']
carga_blue_API['appartment'] = carga_blue_API['floor_number']

# Creación de campos fijos
carga_blue_API['custom_1'] = 'ROLLITOS SOFT'

# Creación de campos vacíos obligatorios CW
carga_blue_API['oc'] = ''
carga_blue_API['alloc_qty'] = 0
carga_blue_API['odc'] = ''
carga_blue_API['company_id'] = 0
carga_blue_API['customfilter_5'] = ''
carga_blue_API['created_at'] = ''
carga_blue_API['updated_at'] = ''
carga_blue_API['pending_qty'] = 0
carga_blue_API['invoiced_qty'] = 0
carga_blue_API['odd_qty'] = 0
carga_blue_API['canceled_qty'] = 0
carga_blue_API['picking_qty'] = 0
carga_blue_API['invoiced_price'] = 0
carga_blue_API['custom_1'] = ''
carga_blue_API['custom_2'] = ''
carga_blue_API['custom_3'] = ''
carga_blue_API['custom_4'] = ''
carga_blue_API['custom_5'] = ''
carga_blue_API['custom_6'] = ''
carga_blue_API['company'] = ''
carga_blue_API['shipping_facility_id'] = 0
carga_blue_API['shipping_facility'] = 0
carga_blue_API['rut'] = ''
carga_blue_API['client'] = 'TRANSBANK'
carga_blue_API['client_receiving_facility_id'] = 0
carga_blue_API['client_receiving_facility'] = 0
carga_blue_API['status'] = 0
carga_blue_API['details_in'] = 0
carga_blue_API['details_out'] = 0
carga_blue_API['is_completed'] = ''
carga_blue_API['cod_amount'] = ''
carga_blue_API['receiver'] = ''
carga_blue_API['postal_code'] = ''
carga_blue_API['phone'] = ''
carga_blue_API['prefix_phone'] = ''
carga_blue_API['prefix_cellphone'] = ''
carga_blue_API['email'] = ''
carga_blue_API['owner'] = ''
carga_blue_API['comment'] = ''
carga_blue_API['transport'] = ''
carga_blue_API['service_type'] = ''
carga_blue_API['informed_status'] = ''
carga_blue_API['client_item'] = ''
carga_blue_API['line'] = 1
carga_blue_API['item_nbr_2'] = ''
carga_blue_API['oc_id'] = 0

# Cambiar el tipo de dato de la columna 'Columna_Objeto' a cadena (str)
'''
carga_blue_API['custom_1'] = carga_blue_API['custom_1'].astype(str)
carga_blue_API['custom_2'] = carga_blue_API['custom_2'].astype(str)
carga_blue_API['custom_3'] = carga_blue_API['custom_3'].astype(str)
carga_blue_API['custom_4'] = carga_blue_API['custom_4'].astype(str)
carga_blue_API['custom_5'] = carga_blue_API['custom_5'].astype(str)
carga_blue_API['custom_6'] = carga_blue_API['custom_6'].astype(str)
'''

# Filtrar df según valores obligatorios para API
carga_blue_API = carga_blue_API[['order_nbr_id','line','item_nbr','item_nbr_2','item_desc','client_item','ordered_date',
                                         'shipping_date','ordered_qty','alloc_qty','pending_qty','invoiced_qty','odd_qty',
                                         'canceled_qty','picking_qty','invoiced_price','custom_1','custom_2','custom_3',
                                         'custom_4','custom_5','custom_6','order_nbr','oc_id','oc','odc','doctype','company_id',
                                         'company','shipping_facility_id','shipping_facility','rut','client',
                                         'client_receiving_facility_id','client_receiving_facility','customfilter_5',
                                         'status','created_at','updated_at','details_in',
                                         'details_out','is_completed','receiver','region','sector','locality','address',
                                         'street_number','floor_number','appartment','postal_code','phone','prefix_phone',
                                         'cellphone','prefix_cellphone','email','owner','comment','transport','service_type']]

print(carga_blue_API['custom_1'].dtypes)

# Crear un diccionario anidado
nested_json = {"data": []}

# Recorrer el DataFrame fila por fila
for index, row in carga_blue_API.iterrows():
    order_detail = {
        "orderdetail_set": [
            {
                "order_nbr_id": str(row['order_nbr_id']),
                "line": float(row['line']),
                "item_nbr": str(row['item_nbr']),
                "item_nbr_2": str(row['item_nbr_2']),
                "item_desc": str(row['item_desc']),
                "client_item": str(row['client_item']),
                "ordered_date": str(row['ordered_date']),
                "shipping_date": str(row['shipping_date']),
                "ordered_qty": int(row['ordered_qty']),
                "alloc_qty": int(row['alloc_qty']),
                "pending_qty": int(row['pending_qty']),
                "invoiced_qty": int(row['invoiced_qty']),
                "odd_qty": int(row['odd_qty']),
                "canceled_qty": int(row['canceled_qty']),
                "picking_qty": int(row['picking_qty']),
                "invoiced_price": int(row['invoiced_price']),
                "custom_1": str(row['custom_1']),
                "custom_2": str(row['custom_2']),
                "custom_3": str(row['custom_3']),
                "custom_4": str(row['custom_4']),
                "custom_5": str(row['custom_5']),
                "custom_6": str(row['custom_6']),                
            }
        ],
        "ordered_date": str(row['ordered_date']),
        "shipping_date": str(row['shipping_date']),
        "order_nbr_id": str(row['order_nbr_id']),
        "order_nbr": str(row['order_nbr']),
        "oc_id": str(row['oc_id']),
        "oc": str(row['oc']),
        "odc": str(row['odc']),
        "doctype": str(row['doctype']),
        "company_id": str(row['company_id']),
        "company": str(row['company']),
        "shipping_facility_id": str(row['shipping_facility_id']),
        "shipping_facility": str(row['shipping_facility']),
        "rut": str(row['rut']),
        "client": str(row['client']),
        "client_receiving_facility_id": str(row['client_receiving_facility_id']),
        "client_receiving_facility": str(row['client_receiving_facility']),
        "custom_1": str(row['custom_1']),
        "custom_2": str(row['custom_2']),
        "custom_3": str(row['custom_3']),
        "custom_4": str(row['custom_4']),
        "customfilter_5": str(row['customfilter_5']),
        "status": int(row['status']),
        "created_at": str(row['created_at']),
        "updated_at": str(row['updated_at']),
        "details_in": str(row['details_in']),
        "details_out": str(row['details_out']),
        "is_completed": bool(row['is_completed']),
        "receiver": str(row['receiver']),
        "region": str(row['region']),
        "sector": str(row['sector']),
        "locality": str(row['locality']),
        "address": str(row['address']),
        "street_number": str(row['street_number']),
        "floor_number": str(row['floor_number']),
        "appartment": str(row['appartment']),
        "postal_code": str(row['postal_code']),
        "phone": str(row['phone']),
        "prefix_phone": str(row['prefix_phone']),
        "cellphone": str(row['cellphone']),
        "prefix_cellphone": str(row['prefix_cellphone']),
        "email": str(row['email']),
        "owner": str(row['owner']),
        "comment": str(row['comment']),
        "transport": str(row['transport']),
        "service_type": str(row['service_type']),
        
    }

    nested_json["data"].append(order_detail)

# Guardar el diccionario anidado como un archivo JSON
with open('output_nested.json', 'w') as json_file:
    salida_json = json.dumps(nested_json, indent=2)

# Exporta el DataFrame a un archivo CSV
directorio = "C:/Users/manuel.manodes/Documents/Desarrollos/Local/Transbank/Auto/Salidas/"
carga_blue_API.to_csv(f"{directorio}/Carga Blue API {fecha_actual} .csv", index=False, encoding='latin1',sep=';', decimal=',', quotechar='"', quoting=2, header=True)

