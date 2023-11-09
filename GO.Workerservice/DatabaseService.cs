namespace GO.Workerservice;

using Microsoft.Extensions.Logging;
using System.Data.Odbc;

public class DatabaseService 
{
    private readonly DatabaseConfiguration DatabaseConfiguration;
    private readonly ILogger? Logger;
    private OdbcConnection? Connection;

    public DatabaseService(DatabaseConfiguration databaseConfiguration, ILogger? logger = null) 
    {
        this.DatabaseConfiguration = databaseConfiguration;
        this.Logger = logger;
    }
    public void Connect() {
        this.Connection = new()
        {
            ConnectionString = this.DatabaseConfiguration.GenerateConnectionString()
        };

        try 
        {
            Connection.Open();
            this.Logger?.LogInformation("Connected to database.");
        } 
        catch (OdbcException e) 
        {
            this.Logger?.LogError("An error has occurred while trying to connect to databse.");
            this.Logger?.LogDebug(e.Message);
            System.Environment.Exit(1);
        }
    }

    public void Close() 
    {
        if (this.Connection != null) 
        {
            this.Connection.Close();
            this.Logger?.LogInformation("Database connection closed.");
        }
    }


    public void GetOrder(string FreightLetterNumber) {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT FIRST *,
                            (select if df_kz_go>0 then 'go'+lower(df_hstat) else null endif from dba.tb_stationen
                            where df_stat=df_abstat) zieldb1,
                            (select if df_kz_go>0 then 'go'+lower(df_hstat) else null endif from dba.tb_stationen
                            where df_stat=df_empfstat) zieldb,
                            current database origdb
                            FROM DBA.TB_AUFTRAG
                            WHERE df_pod=FREIGHT_LETTER_NUMBER
                            AND df_datauftannahme BETWEEN current date-3 AND current date
                            AND df_abstat!='XXX' AND df_empfstat!='XXX'
                            ORDER BY df_datauftannahme DESC";

        OdbcParameter param = new()
        {
            ParameterName = "@FREIGHT_LETTER_NUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = FreightLetterNumber
        };

        cmd.Parameters.Add(param);

        using (OdbcDataReader reader = cmd.ExecuteReader())
        {
            if (reader.HasRows)
            {
                while (reader.Read())
                {
                    Console.WriteLine("{0}: {1:C}", reader[0], reader[1]);
                }
            }
            else
            {
                Console.WriteLine("No rows found.");
            }
            reader.Close();
        }
    }

    public async void GetScan(string FreightLetterNumber) {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT * FROM DBA.TB_SCAN
                            where df_pod=FREIGHT_LETTER_NUMBER
                            and df_scananlass=30
                            and df_packnr=1
                            and df_abstat='FRA'
                            and df_empfstat='MUC'
                            and df_linnr=53
                            and df_scandat between current date-3 and current date;";

        OdbcParameter param = new()
        {
            ParameterName = "@FREIGHT_LETTER_NUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = FreightLetterNumber
        };

        cmd.Parameters.Add(param);

        await cmd.ExecuteReaderAsync();
    }

    public async void AddScanAsync() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO DBA.TB_SCAN
                            (df_abstat, df_empfstat, df_linnr, df_pod, df_packnr, df_scandat, df_scantime,
                            df_scanort, df_scananlass, df_errcode, df_platznr, df_user, df_gewicht, df_kfznr,
                            df_datschicht, df_origdb, df_zieldb, df_zieldb1, df_hub, df_zieldb2, df_timestamp,
                            df_dispoan, df_manuell, df_zieldb_auftraggeber, df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_laenge, df_breite, df_hoehe)
                            VALUES
                            ('FRA', 'MUC',53,'068007339524',1,'2019-02-
                            15','18:01:28.328','TXL',30,'',0,'akl',15.50,'APA325','2019-02-15',current
                            database,null,null,'RH6',null,'2019-02-15 18:01:28.328',0,'N',null,'TXL','2019-02-
                            12',551,61,51,41);";

        await cmd.ExecuteNonQueryAsync();
    }

    public async void Command4() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT SUM(df_gewicht) AS totalweight
                            FROM DBA.TB_SCAN
                            where df_pod='068007339524'
                            and df_scananlass=30
                            and df_abstat='FRA'
                            and df_empfstat='MUC'
                            and df_linnr=53
                            and df_scandat between current date-3 and current date;";

        await cmd.ExecuteReaderAsync();
    }

    public async void Command5() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_real_kg = 15.500
                            WHERE df_ndl='TXL'
                            and df_datauftannahme='2019-02-12'
                            and df_lfdnrauftrag=551;";

        await cmd.ExecuteNonQueryAsync();
    }


    public async void Command6() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT * FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl='TXL'
                            and df_datauftannahme='2019-02-12'
                            and df_lfdnrauftrag=551
                            and df_lfdnrpack = 1;";

        await cmd.ExecuteReaderAsync();
    }


    public async void Command7() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK (df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_lfdnrpack, df_laenge, df_breite, df_hoehe, df_volkg, df_hwb_nr,
                            df_origdb, df_zieldb, df_replikation, df_zieldb1, df_timestamp) VALUES ('TXL', '2019-
                            02-12',551,1,61,51,41, 25.510, '068007339524', current database, 'gomuc', 1, 'gofra',
                            current timestamp);";

        await cmd.ExecuteNonQueryAsync();
    }

    public async void Command8() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT SUM(df_volkg) AS totalvolumeweight
                            FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl='TXL'
                            and df_datauftannahme='2019-02-12'
                            and df_lfdnrauftrag=551;";

        await cmd.ExecuteReaderAsync();
    }

    public async void Command9() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_volkg = 25.510
                            WHERE df_ndl='TXL'
                            and df_datauftannahme='2019-02-12'
                            and df_lfdnrauftrag=551;";

        await cmd.ExecuteNonQueryAsync();
    }

    public async void Command10() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df__kg = ERMITTELTES_GEWICHT
                            WHERE df_ndl='TXL'
                            and df_datauftannahme='2019-02-12'
                            and df_lfdnrauftrag=551;";

        await cmd.ExecuteNonQueryAsync();
    }
}