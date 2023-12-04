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
                            WHERE df_pod=?
                            AND df_datauftannahme BETWEEN current date-3 AND current date
                            AND df_abstat!='XXX' AND df_empfstat!='XXX'
                            ORDER BY df_datauftannahme DESC";

        cmd.Parameters.Add("@df_pod", OdbcType.Int).Value = FreightLetterNumber;

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

    public async Task<ScanData?> GetScanAsync(PackageData data) // 2
    {
        if (this.Connection == null) return null;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT * FROM DBA.TB_SCAN
                            where df_pod=?
                            and df_scananlass=30
                            and df_packnr=?
                            and df_abstat=?
                            and df_empfstat=?
                            and df_linnr=?
                            and df_scandat between current date-3 and current date;";

        cmd.Parameters.Add("@df_pod", OdbcType.Int).Value = data.df_pod;
        cmd.Parameters.Add("@df_packnr", OdbcType.Int).Value = data.df_scananlass;
        cmd.Parameters.Add("@df_abstat", OdbcType.VarChar).Value = data.df_abstat;
        cmd.Parameters.Add("@df_empfstat", OdbcType.VarChar).Value = data.df_empfstat;
        cmd.Parameters.Add("@df_linnr", OdbcType.Int).Value = data.df_linnr;

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
                            (?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?);";

        cmd.Parameters.Add("@df_abstat", OdbcType.VarChar).Value = packageData.df_abstat;
        cmd.Parameters.Add("@df_empfstat", OdbcType.VarChar).Value = packageData.df_empfstat;
        cmd.Parameters.Add("@df_linnr", OdbcType.VarChar).Value = packageData.df_linnr;
        cmd.Parameters.Add("@df_pod", OdbcType.VarChar).Value = packageData.df_pod;
        cmd.Parameters.Add("@df_packnr", OdbcType.VarChar).Value = packageData.df_packnr;
        cmd.Parameters.Add("@df_scandat", OdbcType.VarChar).Value = packageData.df_scandat;
        cmd.Parameters.Add("@df_scantime", OdbcType.VarChar).Value = packageData.df_scantime;
        cmd.Parameters.Add("@df_scanort", OdbcType.VarChar).Value = packageData.df_scanort;
        cmd.Parameters.Add("@df_scananlass", OdbcType.VarChar).Value = packageData.df_scananlass;
        cmd.Parameters.Add("@df_errcode", OdbcType.VarChar).Value = packageData.df_errcode;
        cmd.Parameters.Add("@df_platznr", OdbcType.VarChar).Value = packageData.df_platznr;
        cmd.Parameters.Add("@df_user", OdbcType.VarChar).Value = packageData.df_user;
        cmd.Parameters.Add("@df_gewicht", OdbcType.VarChar).Value = scaleDimensionerResult.Weight;
        cmd.Parameters.Add("@df_kfznr", OdbcType.VarChar).Value = packageData.df_kfznr;
        cmd.Parameters.Add("@df_datschicht", OdbcType.VarChar).Value = packageData.df_datschicht;
        cmd.Parameters.Add("@df_origdb", OdbcType.VarChar).Value = packageData.df_origdb;
        cmd.Parameters.Add("@df_zieldb", OdbcType.VarChar).Value = packageData.df_zieldb;
        cmd.Parameters.Add("@df_zieldb1", OdbcType.VarChar).Value = packageData.df_zieldb1;
        cmd.Parameters.Add("@df_hub", OdbcType.VarChar).Value = packageData.df_hub;
        cmd.Parameters.Add("@df_zieldb2", OdbcType.VarChar).Value = packageData.df_zieldb2;
        cmd.Parameters.Add("@df_timestamp", OdbcType.VarChar).Value = packageData.df_timestamp;
        cmd.Parameters.Add("@df_dispoan", OdbcType.VarChar).Value = packageData.df_dispoan;
        cmd.Parameters.Add("@df_manuell", OdbcType.VarChar).Value = packageData.df_manuell;
        cmd.Parameters.Add("@df_zieldb_auftraggeber", OdbcType.VarChar).Value = packageData.df_zieldb_auftraggeber;
        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = packageData.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = packageData.df_datauftannahme;
        cmd.Parameters.Add("@df_lfdnrauftrag", OdbcType.VarChar).Value = packageData.df_lfdnrauftrag;
        cmd.Parameters.Add("@df_laenge", OdbcType.VarChar).Value = scaleDimensionerResult.Length;
        cmd.Parameters.Add("@df_breite", OdbcType.VarChar).Value = scaleDimensionerResult.Width;
        cmd.Parameters.Add("@df_hoehe", OdbcType.VarChar).Value = scaleDimensionerResult.Height;
        
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task UpdateScanAsync(ScaleDimensionerResult scaleDimensionerResult, PackageData packageData) // 3.1
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
                            (?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?,
                            ?, ?, ?, ?);";

        cmd.Parameters.Add("@df_abstat", OdbcType.VarChar).Value = packageData.df_abstat;
        cmd.Parameters.Add("@df_empfstat", OdbcType.VarChar).Value = packageData.df_empfstat;
        cmd.Parameters.Add("@df_linnr", OdbcType.VarChar).Value = packageData.df_linnr;
        cmd.Parameters.Add("@df_pod", OdbcType.VarChar).Value = packageData.df_pod;
        cmd.Parameters.Add("@df_packnr", OdbcType.VarChar).Value = packageData.df_packnr;
        cmd.Parameters.Add("@df_scandat", OdbcType.VarChar).Value = packageData.df_scandat;
        cmd.Parameters.Add("@df_scantime", OdbcType.VarChar).Value = packageData.df_scantime;
        cmd.Parameters.Add("@df_scanort", OdbcType.VarChar).Value = packageData.df_scanort;
        cmd.Parameters.Add("@df_scananlass", OdbcType.VarChar).Value = packageData.df_scananlass;
        cmd.Parameters.Add("@df_errcode", OdbcType.VarChar).Value = packageData.df_errcode;
        cmd.Parameters.Add("@df_platznr", OdbcType.VarChar).Value = packageData.df_platznr;
        cmd.Parameters.Add("@df_user", OdbcType.VarChar).Value = packageData.df_user;
        cmd.Parameters.Add("@df_gewicht", OdbcType.VarChar).Value = scaleDimensionerResult.Weight;
        cmd.Parameters.Add("@df_kfznr", OdbcType.VarChar).Value = packageData.df_kfznr;
        cmd.Parameters.Add("@df_datschicht", OdbcType.VarChar).Value = packageData.df_datschicht;
        cmd.Parameters.Add("@df_origdb", OdbcType.VarChar).Value = packageData.df_origdb;
        cmd.Parameters.Add("@df_zieldb", OdbcType.VarChar).Value = packageData.df_zieldb;
        cmd.Parameters.Add("@df_zieldb1", OdbcType.VarChar).Value = packageData.df_zieldb1;
        cmd.Parameters.Add("@df_hub", OdbcType.VarChar).Value = packageData.df_hub;
        cmd.Parameters.Add("@df_zieldb2", OdbcType.VarChar).Value = packageData.df_zieldb2;
        cmd.Parameters.Add("@df_timestamp", OdbcType.VarChar).Value = packageData.df_timestamp;
        cmd.Parameters.Add("@df_dispoan", OdbcType.VarChar).Value = packageData.df_dispoan;
        cmd.Parameters.Add("@df_manuell", OdbcType.VarChar).Value = packageData.df_manuell;
        cmd.Parameters.Add("@df_zieldb_auftraggeber", OdbcType.VarChar).Value = packageData.df_zieldb_auftraggeber;
        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = packageData.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = packageData.df_datauftannahme;
        cmd.Parameters.Add("@df_lfdnrauftrag", OdbcType.VarChar).Value = packageData.df_lfdnrauftrag;
        cmd.Parameters.Add("@df_laenge", OdbcType.VarChar).Value = scaleDimensionerResult.Length;
        cmd.Parameters.Add("@df_breite", OdbcType.VarChar).Value = scaleDimensionerResult.Width;
        cmd.Parameters.Add("@df_hoehe", OdbcType.VarChar).Value = scaleDimensionerResult.Height;
        
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<int?> GetWeightAsync(PackageData data) // 4
    {
        if (this.Connection == null) return null;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT SUM(df_gewicht) AS totalweight
                            FROM DBA.TB_SCAN
                            where df_pod=?
                            and df_scananlass=30
                            and df_abstat=?
                            and df_empfstat=?
                            and df_linnr=?
                            and df_scandat between current date-3 and current date;";

        cmd.Parameters.Add("@df_pod", OdbcType.VarChar).Value = data.df_pod;
        cmd.Parameters.Add("@df_abstat", OdbcType.VarChar).Value = data.df_abstat;
        cmd.Parameters.Add("@df_empfstat", OdbcType.VarChar).Value = data.df_empfstat;
        cmd.Parameters.Add("@df_linnr", OdbcType.VarChar).Value = data.df_linnr;

        await cmd.ExecuteReaderAsync();

        return 0;
    }

    public async Task UpdateWeightAsync(int weight, PackageData data) // 5
    {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_real_kg = ?
                            WHERE df_ndl=?
                            and df_datauftannahme=?
                            and df_lfdnrauftrag=?;";

        cmd.Parameters.Add("@df_real_kg", OdbcType.Int).Value = weight;
        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;

        await cmd.ExecuteNonQueryAsync();
    }


    public async Task<bool?> DoesPackageExistAsync(PackageData data) // 6
    {
        if (this.Connection == null) return null;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT * FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl=?
                            and df_datauftannahme=?
                            and df_lfdnrauftrag=?
                            and df_lfdnrpack = ?;";

        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;
        cmd.Parameters.Add("@df_df_lfdnrpack", OdbcType.Int).Value = data.df_lfdnrpack;

        using OdbcDataReader reader = cmd.ExecuteReader();
        return reader.HasRows;
    }


    public async Task CreatePackageAsync(PackageData data, ScaleDimensionerResult scaleDimensionerResult, float volumeFactor) // 8
    {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK (df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_lfdnrpack, df_laenge, df_breite, df_hoehe, df_volkg, df_hwb_nr,
                            df_origdb, df_zieldb, df_replikation, df_zieldb1, df_timestamp) VALUES (?, ?,
                            ?, ?, ?, ?, ?, current database, ?, ?, ?,
                            current timestamp);";

        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;
        cmd.Parameters.Add("@df_lfdnrpack", OdbcType.Int).Value = data.df_lfdnrpack;
        cmd.Parameters.Add("@df_laenge", OdbcType.Int).Value = scaleDimensionerResult.Length;
        cmd.Parameters.Add("@df_breite", OdbcType.Int).Value = scaleDimensionerResult.Width;
        cmd.Parameters.Add("@df_hoehe", OdbcType.Int).Value = scaleDimensionerResult.Height;
        cmd.Parameters.Add("@df_volkg", OdbcType.VarChar).Value = scaleDimensionerResult.Weight;
        cmd.Parameters.Add("@df_hwb_nr", OdbcType.VarChar).Value = data.df_hwb_nr;
        cmd.Parameters.Add("@df_origdb", OdbcType.VarChar).Value = data.df_origdb;
        cmd.Parameters.Add("@df_zieldb", OdbcType.VarChar).Value = data.df_zieldb;
        cmd.Parameters.Add("@df_replikation", OdbcType.Int).Value = data.df_replikation;
        cmd.Parameters.Add("@df_zieldb1", OdbcType.VarChar).Value = data.df_zieldb1;
        cmd.Parameters.Add("@df_timestamp", OdbcType.VarChar).Value = data.df_timestamp;

        await cmd.ExecuteNonQueryAsync();
    }
    public async Task UpdatePackageAsync(PackageData data, ScaleDimensionerResult scaleDimensionerResult, float volumeFactor) // 8.1
    {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO DBA.TB_AUFTRAGSPACKSTUECK (df_ndl, df_datauftannahme,
                            df_lfdnrauftrag, df_lfdnrpack, df_laenge, df_breite, df_hoehe, df_volkg, df_hwb_nr,
                            df_origdb, df_zieldb, df_replikation, df_zieldb1, df_timestamp) VALUES (?, ?,
                            ?, ?, ?, ?, ?, current database, ?, ?, ?, current timestamp);";

        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;
        cmd.Parameters.Add("@df_lfdnrpack", OdbcType.Int).Value = data.df_lfdnrpack;
        cmd.Parameters.Add("@df_laenge", OdbcType.Int).Value = scaleDimensionerResult.Length;
        cmd.Parameters.Add("@df_breite", OdbcType.Int).Value = scaleDimensionerResult.Width;
        cmd.Parameters.Add("@df_hoehe", OdbcType.Int).Value = scaleDimensionerResult.Height;
        cmd.Parameters.Add("@df_volkg", OdbcType.VarChar).Value = scaleDimensionerResult.Weight;
        cmd.Parameters.Add("@df_hwb_nr", OdbcType.VarChar).Value = data.df_hwb_nr;
        cmd.Parameters.Add("@df_origdb", OdbcType.VarChar).Value = data.df_origdb;
        cmd.Parameters.Add("@df_zieldb", OdbcType.VarChar).Value = data.df_zieldb;
        cmd.Parameters.Add("@df_replikation", OdbcType.Int).Value = data.df_replikation;
        cmd.Parameters.Add("@df_zieldb1", OdbcType.VarChar).Value = data.df_zieldb1;
        cmd.Parameters.Add("@df_timestamp", OdbcType.VarChar).Value = data.df_timestamp;

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<int?> GetTotalWeightAsync(PackageData data) // 9
    {
        if (this.Connection == null) return null;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"SELECT SUM(df_volkg) AS totalvolumeweight
                            FROM DBA.TB_AUFTRAGSPACKSTUECK
                            where df_ndl=?
                            and df_datauftannahme=?
                            and df_lfdnrauftrag=?;";


        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;

        await cmd.ExecuteReaderAsync();

        return 0;
    }

    public async Task UpdateTotalWeightAsync(int totalweight, PackageData data) // 10
    {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df_volkg = ?
                            WHERE df_ndl=?
                            and df_datauftannahme=?
                            and df_lfdnrauftrag=?;";

        cmd.Parameters.Add("@df_volkg", OdbcType.Int).Value = totalweight;
        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;

        await cmd.ExecuteNonQueryAsync();
    }

    public async Task UpdateOrderWeightAsync(int weight, PackageData data) // 11
    {
        if (this.Connection == null) return;

        OdbcCommand cmd = Connection.CreateCommand();
        cmd.CommandText = @"UPDATE DBA.TB_AUFTRAG
                            SET df__kg = ?
                            WHERE df_ndl=?
                            and df_datauftannahme=?
                            and df_lfdnrauftrag=?;";

        cmd.Parameters.Add("@df_kg", OdbcType.Int).Value = weight;
        cmd.Parameters.Add("@df_ndl", OdbcType.VarChar).Value = data.df_ndl;
        cmd.Parameters.Add("@df_datauftannahme", OdbcType.VarChar).Value = data.df_datauftannahme;
        cmd.Parameters.Add("@df_df_lfdnrauftrag", OdbcType.Int).Value = data.df_lfdnrauftrag;

        await cmd.ExecuteNonQueryAsync();
    }
}