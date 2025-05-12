from sqlalchemy import TEXT, Text, DateTime, String

# Mapeamento dos tipos de dados para as tabelas
SCHEMA = {
    # Outras tabelas
    'silver.d_blocos': {
        'co_pais': TEXT,
        'co_bloco': TEXT,
        'no_bloco': Text,
        'no_bloco_ing': Text,
        'no_bloco_esp': Text,
        'data_carga': DateTime
    },
    'silver.d_ncm': {
        'co_ncm': String(25),
        'co_unid': TEXT,
        'co_sh6': String(25),
        'co_ppe': String(25),
        'co_ppi': String(25),
        'co_fat_agreg': String(25),
        'co_cuci_item': String(25),
        'co_cgce_n3': String(25),
        'co_siit': String(25),
        'co_isic_classe': String(25),
        'co_exp_subset': String(25),
        'no_ncm_por': Text,
        'no_ncm_esp': Text,
        'no_ncm_ing': Text,
        'data_carga': DateTime
    },
    'silver.d_ncm_isic': {
        'co_isic_classe': TEXT,
        'no_isic_classe': Text,
        'no_isic_classe_ing': Text,
        'no_isic_classe_esp': Text,
        'co_isic_grupo': TEXT,
        'no_isic_grupo': Text,
        'no_isic_grupo_ing': Text,
        'no_isic_grupo_esp': Text,
        'co_isic_divisao': TEXT,
        'no_isic_divisao': Text,
        'no_isic_divisao_ing': Text,
        'no_isic_divisao_esp': Text,
        'co_isic_secao': Text,
        'no_isic_secao': Text,
        'no_isic_secao_ing': Text,
        'no_isic_secao_esp': Text,
        'data_carga': DateTime
    },
    'silver.d_ncm_sh': {
        'co_sh6': TEXT,
        'no_sh6_por': Text,
        'no_sh6_esp': Text,
        'no_sh6_ing': Text,
        'co_sh4': TEXT,
        'no_sh4_por': Text,
        'no_sh4_esp': Text,
        'no_sh4_ing': Text,
        'co_sh2': TEXT,
        'no_sh2_por': Text,
        'no_sh2_esp': Text,
        'no_sh2_ing': Text,
        'co_ncm_secrom': Text,
        'no_sec_por': Text,
        'no_sec_esp': Text,
        'no_sec_ing': Text,
        'data_carga': DateTime
    },
    'silver.d_pais': {
        'co_pais': TEXT,
        'co_pais_ison3': TEXT,
        'co_pais_isoa3': Text,
        'no_pais': Text,
        'no_pais_ing': Text,
        'no_pais_esp': Text,
        'data_carga': DateTime
    },
    'silver.d_uf': {
        'co_uf': TEXT,
        'sg_uf': Text,
        'no_uf': Text,
        'no_regiao': Text,
        'data_carga': DateTime
    },
    'silver.d_unidade': {
        'co_unid': TEXT,
        'no_unid': Text,
        'sg_unid': Text,
        'data_carga': DateTime
    },
    'silver.d_urf': {
        'co_urf': TEXT,
        'no_urf': Text,
        'data_carga': DateTime
    },
    'silver.d_via': {
        'co_via': TEXT,
        'no_via': Text,
        'data_carga': DateTime
    },
    'silver.f_exportacoes_estados': {
        'co_ano': TEXT,
        'co_mes': TEXT,
        'co_ncm': Text,
        'co_unid': Text,
        'co_pais': Text,
        'sg_uf_ncm': Text,
        'co_via': Text,
        'co_urf': Text,
        'qt_estat': TEXT,
        'kg_liquido': TEXT,
        'vl_fob': TEXT,
        'data_carga': DateTime
    },
    'silver.f_importacoes_estados': {
        'co_ano': TEXT,
        'co_mes': TEXT,
        'co_ncm': Text,
        'co_unid': Text,
        'co_pais': Text,
        'sg_uf_ncm': Text,
        'co_via': Text,
        'co_urf': Text,
        'qt_estat': TEXT,
        'kg_liquido': TEXT,
        'vl_fob': TEXT,
        'vl_frete': TEXT,
        'vl_seguro': TEXT,
        'data_carga': DateTime
    },

    'gold.fact_comercio_exterior': {
        'co_ano': TEXT,
        'co_mes': TEXT,
        'co_ncm': TEXT,
        'co_pais': TEXT,
        'sg_uf_ncm': TEXT,
        'co_via': TEXT,
        'co_urf': TEXT,
        'kg_liquido': TEXT,
        'vl_fob': TEXT, 
        'tipo_mov': TEXT
    }
}
