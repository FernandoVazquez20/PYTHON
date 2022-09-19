import pandas as pd
import sqlalchemy


data = pd.read_csv('info\incidentes_viales_feb_2022.csv', encoding='latin1', on_bad_lines='skip',low_memory=False)



def creacion_tablas(df,table_name):
    """
    Func name: Creacion tablas

    input: 
            df: dataframe to manipulate
            table_name: str object , specify the table name to create
    output:
            insert data into database SQL Server
    """
    try:

        engine = sqlalchemy.create_engine('mssql://MSI/AccidentesViales?trusted_connection=yes')
        cursor = engine.connect()
        print('conexiòn exitosa')
    except:
        print('error al intentar conectarse')

    if table_name == "concepto":

        copia_concepto = df.loc[:,["folio","incidente_c4","clas_con_f_alarma","tipo_entrada","codigo_cierre"]]
        ent_concepto = copia_concepto.drop_duplicates().sort_index().reset_index(drop=True)
        ent_concepto.insert(0,'id',ent_concepto.index+1)

        val_sucio_incidente_c4 ={'cadÃ¡ver-atropellado':'cadaver atropellado', 'detenciÃ³n ciudadana-atropellado':'detencion cuidadana-atropellado',
                            'cadÃ¡ver-accidente automovilÃ\xadstico':'cadaver-accidente automovilistico','accidente-vehÃ\xadculo atrapado-varado':'accidente-vehiculo atrapado-varado',
                            'mi ciudad-calle-incidente de trÃ¡nsito':'mi ciudad-calle-incidente de transito','detenciÃ³n ciudadana-accidente automovilÃ\xadstico':'detencion ciudadana-accidente automovilistico',
                            'mi ciudad-taxi-incidente de trÃ¡nsito':'mi ciudad-taxi-incidente de transito'}

        val_sucio_tipo_entrada = {'BOTÃ\x93N DE AUXILIO':'Boton auxilio','CÃ\x81MARA':'camara',
                                 'MiÃ©rcoles':'miercoles','cadÃ¡ver-atropellado':'atropellado','MiÃ©rcol1s':'miercoles',
                                 'MiÃ©rco':'miercoles','SÃ¡bado':'sabado'}

        ent_concepto.replace(val_sucio_incidente_c4,
           inplace=True)
        ent_concepto.replace(val_sucio_tipo_entrada,inplace=True)
        for index, row in ent_concepto.iterrows():
            cursor.execute("INSERT INTO concepto ([id_incidente],[folio_fk],[incidente_c4],[clas_con_f_alarma],[tipo_entrada],[codigo_cierre]) VALUES (?,?,?,?,?,?)"
                            ,row.id,row.folio,row.incidente_c4,row.clas_con_f_alarma,row.tipo_entrada,row.codigo_cierre)
        engine.commit()
        cursor.close()

                

    elif table_name == "Delegacion":
        copia_delegacion = df.loc[:,["folio","delegacion_inicio","latitud","longitud","delegacion_cierre"]]
        ent_delegacion =  copia_delegacion.drop_duplicates().sort_index().reset_index(drop=True)
        ent_delegacion.insert(0,'id',ent_delegacion.index+1)
        for index, row in ent_delegacion.iterrows():
            cursor.execute("INSERT INTO Delegacion ([folio],[delegacion_inicio],[latitud],[lonitug],[delegacion_cierre]) VALUES (?,?,?,?,?)"
                            ,row.folio,row.delegacion_inicio,row.latitud,row.longitud,row.delegacion_cierre)
        engine.commit()
        cursor.close()
       

    elif table_name == "tiempo":
        copia_tiempo = df.loc[:,["folio","fecha_creacion","hora_creacion","fecha_cierre","hora_cierre","ano_cierre","mes_cierre"]]
        ent_tiempo = copia_tiempo.drop_duplicates().sort_index().reset_index(drop=True)
        ent_tiempo.insert(0,'id',ent_tiempo.index+1)
        val_sucio = {'MiÃ©rcoles':'Miercoles','SÃ¡bado':'Sabado','SÃÃ©rco':'Sabado','MiÃ©rco':'Miercoles','SiÃ©rco':'Sabado','SÃ¡©rco':'Sabado',
                 'MÃ¡bado':'Sabado','SÃ¡baco':'Sabado','CÃ\x81MARA':'No identificado','SÃ¡bARA':'No identificado','SÃ\x81MARA':'No identificado',
                 'MiÃMARA':'No identificado','MiÃ©ARA':'No identificado','SÃ¡badA':'Sabado','SÃ¡baRA':'Sabado','accidente-choque sin lesionados':'No identificado','MiÃ©rcol':'Miercoles',
                 'lesionado-atropellado':'No identificado','SÃ¡brcoles':'Miercoles','MiÃ©rRA':'Miercoles', 'MiÃ©rcolCALCO,19\x18':'Miercoles','MiÃbado':'Sabado', 'MiÃ©rdo':'Miercoles', 'SÃ¡brco':'Sabado',
                  'MiÃ©ado':'Sabado', 'Mi¡bado':'Sabado', 'MÃ\x81MARA':'No identificado','1':'Enero'}
        ent_tiempo.replace(val_sucio,inplace=True)
        for index, row in ent_tiempo.iterrows():
            cursor.execute("INSERT INTO tiempo ([folio],[delegacion_inicio],[latitud],[lonitug],[delegacion_cierre]) VALUES (?,?,?,?,?,?,?)"
                            ,row.folio,row.fecha_creacion,row.hora_creacion,row.fecha_cierre,row.hora_cierre,row.ano_cierre,row.mes_cierre)
        engine.commit()
        cursor.close()

    else:
        print("Nombre de tabla no identificado")
    print("Proceso terminado")


creacion_tablas(data,"Delegacion")
