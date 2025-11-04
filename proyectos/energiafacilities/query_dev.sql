-- esquema de datos crudo.
CREATE SCHEMA IF NOT EXISTS RAW;
-- esquema para datos procesados.
CREATE SCHEMA IF NOT EXISTS ODS;


--Creamos las tablas Raw
-- Tabla  Maestro mensual consumo suministro
CREATE TABLE RAW.SFTP_MM_CONSUMO_SUMINISTRO (
    num_recibo            VARCHAR(255) ,
    cod_suministro        VARCHAR(255),
    tarifa                VARCHAR(255),
    pot_contr             VARCHAR(255),
    titularidad           VARCHAR(255),
    distribuidor          VARCHAR(255),

    fec_emision           VARCHAR(255),
    fec_vencimiento       VARCHAR(255),
    fec_lect_actual       VARCHAR(255),
    fec_lect_anterior     VARCHAR(255),

    per_consumo           VARCHAR(255),

    ea_lec_act_hp         VARCHAR(255),
    ea_lec_act_fp         VARCHAR(255),
    ea_lec_act_tot        VARCHAR(255),
    ea_lec_ant_hp         VARCHAR(255),
    ea_lec_ant_fp         VARCHAR(255),
    ea_lec_ant_tot        VARCHAR(255),

    ea_consumo_hp         VARCHAR(255),
    ea_consumo_fp         VARCHAR(255),
    ea_consumo_tot        VARCHAR(255),

    ea_pu_hp              VARCHAR(255),
    ea_pu_fp              VARCHAR(255),
    ea_prec_unit_tot      VARCHAR(255),

    ea_importe_hp         VARCHAR(255),
    ea_importe_fp         VARCHAR(255),
    ea_importe_tot        VARCHAR(255),

    pot_lect_act_hp       VARCHAR(255),
    pot_lect_act_fp       VARCHAR(255),
    pot_lect_ant_hp       VARCHAR(255),
    pot_lect_ant_fp       VARCHAR(255),

    pot_reg_hp            VARCHAR(255),
    pot_reg_fp            VARCHAR(255),

    pot_gen_fact          VARCHAR(255),
    pot_gen_pu            VARCHAR(255),
    pot_gen_importe       VARCHAR(255),

    pot_dist_fact         VARCHAR(255),
    pot_dist_pu           VARCHAR(255),
    pot_dist_importe      VARCHAR(255),

    pot_bt6_pu            VARCHAR(255),
    pot_bt6_monto         VARCHAR(255),

    er_lec_act            VARCHAR(255),
    er_lec_ant            VARCHAR(255),
    er_cons               VARCHAR(255),
    er_fact               VARCHAR(255),
    er_pu                 VARCHAR(255),
    er_importe            VARCHAR(255),

    calif_cliente         VARCHAR(255),
    dias_punta            VARCHAR(255),
    horas_punta           VARCHAR(255),
    factor_calif          VARCHAR(255),

    cargo_fijo            VARCHAR(255),
    mantenimiento         VARCHAR(255),
    alumbrado             VARCHAR(255),
    recup_energia         VARCHAR(255),
    mora                  VARCHAR(255),
    reconexion            VARCHAR(255),
    ajuste_tarifa         VARCHAR(255),
    dist_factura          VARCHAR(255),
    ajuste_alumb          VARCHAR(255),
    otros_afectos         VARCHAR(255),

    base_imponible        VARCHAR(255),
    igv                   VARCHAR(255),
    tot_periodo           VARCHAR(255),
    electrif_rural        VARCHAR(255),

    comp_distribucion     VARCHAR(255),
    comp_generadora       VARCHAR(255),
    comp_interrup         VARCHAR(255),
    comp_calidad          VARCHAR(255),
    comp_frec_ant         VARCHAR(255),
    comp_frec_des         VARCHAR(255),
    comp_serv             VARCHAR(255),
    comp_norma_tec        VARCHAR(255),
    comp_deuda_ant        VARCHAR(255),
    comp_deuda_meses      VARCHAR(255),
    comp_dev_reclamo      VARCHAR(255),
    comp_nota_deb_cred    VARCHAR(255),
    comp_aporte_reemb     VARCHAR(255),
    comp_otros_inafectos  VARCHAR(255),
    comp_redondeo_act_pos VARCHAR(255),
    comp_redondeo_act_neg VARCHAR(255),

    costo_medio           VARCHAR(255),
    total_a_pagar         VARCHAR(255),
    valor_venta           VARCHAR(255),
    deuda_anterior        VARCHAR(255),
    devolucion            VARCHAR(255),
    fecha_Liquidacion     VARCHAR(255)
);

-- Historico Mensual consumo suministro
CREATE TABLE ODS.SFTP_HM_CONSUMO_SUMINISTRO (
    num_recibo            BIGINT PRIMARY KEY,
    cod_suministro        INTEGER,
    tarifa                VARCHAR(10),
    pot_contr             NUMERIC(18,2),
    titularidad           VARCHAR(255),
    distribuidor          VARCHAR(255),

    fec_emision           DATE,
    fec_vencimiento       DATE,
    fec_lect_actual       DATE,
    fec_lect_anterior     DATE,

    per_consumo           VARCHAR(50),

    ea_lec_act_hp         NUMERIC(18,2),
    ea_lec_act_fp         NUMERIC(18,2),
    ea_lec_act_tot        NUMERIC(18,2),
    ea_lec_ant_hp         NUMERIC(18,2),
    ea_lec_ant_fp         NUMERIC(18,2),
    ea_lec_ant_tot        NUMERIC(18,2),

    ea_consumo_hp         NUMERIC(18,2),
    ea_consumo_fp         NUMERIC(18,2),
    ea_consumo_tot        NUMERIC(18,2),

    ea_pu_hp              NUMERIC(18,6),
    ea_pu_fp              NUMERIC(18,6),
    ea_prec_unit_tot      NUMERIC(18,6),

    ea_importe_hp         NUMERIC(18,2),
    ea_importe_fp         NUMERIC(18,2),
    ea_importe_tot        NUMERIC(18,2),

    pot_lect_act_hp       NUMERIC(18,2),
    pot_lect_act_fp       NUMERIC(18,2),
    pot_lect_ant_hp       NUMERIC(18,2),
    pot_lect_ant_fp       NUMERIC(18,2),

    pot_reg_hp            NUMERIC(18,2),
    pot_reg_fp            NUMERIC(18,2),

    pot_gen_fact          NUMERIC(18,2),
    pot_gen_pu            NUMERIC(18,6),
    pot_gen_importe       NUMERIC(18,2),

    pot_dist_fact         NUMERIC(18,2),
    pot_dist_pu           NUMERIC(18,6),
    pot_dist_importe      NUMERIC(18,2),

    pot_bt6_pu            NUMERIC(18,6),
    pot_bt6_monto         NUMERIC(18,2),

    er_lec_act            NUMERIC(18,2),
    er_lec_ant            NUMERIC(18,2),
    er_cons               NUMERIC(18,2),
    er_fact               NUMERIC(18,2),
    er_pu                 NUMERIC(18,6),
    er_importe            NUMERIC(18,2),

    calif_cliente         VARCHAR(100),
    dias_punta            INTEGER,
    horas_punta           INTEGER,
    factor_calif          VARCHAR(50),

    cargo_fijo            NUMERIC(18,2),
    mantenimiento         NUMERIC(18,2),
    alumbrado             NUMERIC(18,2),
    recup_energia         NUMERIC(18,2),
    mora                  NUMERIC(18,2),
    reconexion            NUMERIC(18,2),
    ajuste_tarifa         NUMERIC(18,2),
    dist_factura          NUMERIC(18,2),
    ajuste_alumb          NUMERIC(18,2),
    otros_afectos         NUMERIC(18,2),

    base_imponible        NUMERIC(18,2),
    igv                   NUMERIC(18,2),
    tot_periodo           NUMERIC(18,2),
    electrif_rural        NUMERIC(18,2),

    comp_distribucion     NUMERIC(18,2),
    comp_generadora       NUMERIC(18,2),
    comp_interrup         NUMERIC(18,2),
    comp_calidad          NUMERIC(18,2),
    comp_frec_ant         NUMERIC(18,2),
    comp_frec_des         NUMERIC(18,2),
    comp_serv             NUMERIC(18,2),
    comp_norma_tec        NUMERIC(18,2),
    comp_deuda_ant        NUMERIC(18,2),
    comp_deuda_meses      INTEGER,
    comp_dev_reclamo      NUMERIC(18,2),
    comp_nota_deb_cred    NUMERIC(18,2),
    comp_aporte_reemb     NUMERIC(18,2),
    comp_otros_inafectos  NUMERIC(18,2),
    comp_redondeo_act_pos NUMERIC(18,2),
    comp_redondeo_act_neg NUMERIC(18,2),

    costo_medio           NUMERIC(18,2),
    total_a_pagar         NUMERIC(18,2),
    valor_venta           NUMERIC(18,2),
    deuda_anterior        NUMERIC(18,2),
    devolucion            NUMERIC(18,2),
    fecha_Liquidacion     DATE
);

--