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


    public async Task<PackageData>? GetOrderAsync(string FreightLetterNumber) // 1
    {
        if (this.Connection == null) return null;

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

        PackageData packageData = null;

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

        return packageData;
    }

    public async Task<ScanData?> GetScanAsync(string FreightLetterNumber) // 2
    {
        if (this.Connection == null) return null;

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

        ScanData scanData = null;

        await cmd.ExecuteReaderAsync();

        return scanData;
    }

    public async Task AddScanAsync(ScaleDimensionerResult scaleDimensionerResult, PackageData packageData) // 3
    {
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
                            12',551,LENGTH,WIDTH,HEIGHT);";

        OdbcParameter weightParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Weight
        };

        OdbcParameter lengthParam = new()
        {
            ParameterName = "@LENGTH",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Length
        };

        OdbcParameter widthParam = new()
        {
            ParameterName = "@WIDTH",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Width
        };

        OdbcParameter heightParam = new()
        {
            ParameterName = "@HEIGTH",
            DbType = System.Data.DbType.VarNumeric,
            Value = scaleDimensionerResult.Height
        };

        cmd.Parameters.Add(weightParam);
        cmd.Parameters.Add(lengthParam);
        cmd.Parameters.Add(widthParam);
        cmd.Parameters.Add(heightParam);

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<int?> GetWeightAsync() // 4
    {
        if (this.Connection == null) return null;

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

        return 0;
    }

    public async Task UpdateWeightAsync(int weight, string scanLocation, string date, string orderNumber) // 5
    {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_real_kg = WEIGHT
                            WHERE df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";

        OdbcParameter weightParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = weight
        };

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.String,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.String,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.String,
            Value = orderNumber
        };

        cmd.Parameters.Add(weightParam);
        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteNonQueryAsync();
    }


    public async Task<bool?> DoesPackageExistAsync(string scanLocation, string date, string orderNumber) // 6
    {
        if (this.Connection == null) return null;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT * FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER
                            and df_lfdnrpack = 1;";

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.VarNumeric,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.VarNumeric,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = orderNumber
        };

        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        using (OdbcDataReader reader = cmd.ExecuteReader())
        {
            return reader.HasRows;
        }
    }


    public async void CreatePackageAsync() {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK (df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_lfdnrpack, df_laenge, df_breite, df_hoehe, df_volkg, df_hwb_nr,
                            df_origdb, df_zieldb, df_replikation, df_zieldb1, df_timestamp) VALUES ('TXL', '2019-
                            02-12',551,1,61,51,41, 25.510, '068007339524', current database, 'gomuc', 1, 'gofra',
                            current timestamp);";

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<int?> GetTotalWeightAsync(string scanLocation, string date, string orderNumber) {
        if (this.Connection == null) return null;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT SUM(df_volkg) AS totalvolumeweight
                            FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";


        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.VarNumeric,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.VarNumeric,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = orderNumber
        };

        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteReaderAsync();

        return 0;
    }

    public async void UpdateOrderVolumeAsync(string volume, string scanLocation, string date, string orderNumber) {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_volkg = VOLUME
                            WHERE df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";

        OdbcParameter volumeParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = volume
        };

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.VarNumeric,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.VarNumeric,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.VarNumeric,
            Value = orderNumber
        };

        cmd.Parameters.Add(volumeParam);
        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteNonQueryAsync();
    }

    public async void UpdateOrderWeightAsync(int weight, string scanLocation, string date, string orderNumber) {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df__kg = WEIGHT
                            WHERE df_ndl='SCANLOCATION'
                            and df_datauftannahme='DATE'
                            and df_lfdnrauftrag=ORDERNUMBER;";

        OdbcParameter weightParam = new()
        {
            ParameterName = "@WEIGHT",
            DbType = System.Data.DbType.VarNumeric,
            Value = weight
        };

        OdbcParameter scanLocationParam = new()
        {
            ParameterName = "@SCANLOCATION",
            DbType = System.Data.DbType.String,
            Value = scanLocation
        };

        OdbcParameter dateParam = new()
        {
            ParameterName = "@DATE",
            DbType = System.Data.DbType.String,
            Value = date
        };

        OdbcParameter orderNumberParam = new()
        {
            ParameterName = "@ORDERNUMBER",
            DbType = System.Data.DbType.String,
            Value = orderNumber
        };

        cmd.Parameters.Add(weightParam);
        cmd.Parameters.Add(scanLocationParam);
        cmd.Parameters.Add(dateParam);
        cmd.Parameters.Add(orderNumberParam);

        await cmd.ExecuteNonQueryAsync();
    }
}