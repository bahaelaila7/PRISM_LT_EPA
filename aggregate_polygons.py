import sqlite3

VARS=['ppt','tmin','tmean','tmax','vpdmin','vpdmax','tdmean']

apply_to_vars = lambda s, d: d.join((s.format(var= var) for var in VARS))
def aggregate_sql(from_table, epa_level= 4, output_monthly=True):
    if epa_level not in [3,4]:
        raise Exception("Only epa levels 3 or 4 are allowed")
    input_monthly_cond = from_table=='monthly'
    output_monthly_cond = input_monthly_cond and output_monthly
    is_monthly = lambda s: s if output_monthly_cond else ''
    output_by = 'yearly' if not output_monthly_cond else 'monthly'
    output_table_name = f'EPA_L{epa_level}_{output_by}_from_{from_table}'
    epa_attr = f'EPA_L{epa_level}_ID'
    var_mean_str = 'sum(PXL_COUNT * {var}_mean)/sum(PXL_COUNT)'

    sql = f'''
    --- calculates supergroup mean (u) and std 
    --- sum(n_i) as N,  sum(n_i * u_i)/N as u, sum(n_i * (std_i +  (u_i - u)^2))/N as std

    --- first calculate the means for supergroups
    DROP TABLE IF EXISTS {output_table_name};
    CREATE TABLE {output_table_name} AS 
    WITH means AS (SELECT {epa_attr}, YEAR{is_monthly(', MONTH')}, sum(PXL_COUNT) PXL_COUNT, 
    {apply_to_vars(f'{var_mean_str} {{var}}_mean', ', ')}
    FROM {from_table}
    GROUP BY {epa_attr}, YEAR{is_monthly(', MONTH')})

    --- now join means and calculate stds
    
    SELECT m.{epa_attr}, m.YEAR{is_monthly(', m.MONTH')}, 
    {apply_to_vars('u.{var}_mean', ', ')}
    ,
    {apply_to_vars("""sqrt(sum(m.PXL_COUNT * ( 
                                        pow(m.{var}_std, 2) + 
                                        pow(u.{var}_mean - m.{var}_mean,2)
                                      )   
                        )/u.PXL_COUNT) {var}_std""",', ')}
    FROM {from_table} m
    JOIN means u ON m.{epa_attr} = u.{epa_attr} AND m.YEAR = u.YEAR{is_monthly(' AND m.MONTH = u.MONTH')}
    GROUP BY m.{epa_attr}, m.YEAR{is_monthly(', m.MONTH')};
    '''
    return output_table_name, sql

if __name__ == '__main__':
    with sqlite3.connect('prism_data_coalesced.db') as conn:
        cur = conn.cursor()
        for epa_level in [3,4]:
            print(f'AGGREGATING EPA_L{epa_level}_POLYGONS')
            print('\tMONTHLY')
            table, sql= aggregate_sql(from_table = 'monthly', epa_level = epa_level, output_monthly = True)
            print('\t',table)
            cur.executescript(sql)
            print('\tYEARLY from yearly')
            table, sql= aggregate_sql(from_table = 'yearly', epa_level = epa_level, output_monthly = False)
            print('\t',table)
            cur.executescript(sql)
            print('\tYEARLY from monthly')
            table, sql= aggregate_sql(from_table = 'monthly', epa_level = epa_level, output_monthly = False)
            print('\t',table)
            cur.executescript(sql)

