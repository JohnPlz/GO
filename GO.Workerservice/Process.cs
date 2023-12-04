using System.Data.Odbc;

namespace GO.Workerservice;

public class Process 
{

    private DatabaseService DatabaseService;
    private ScaleDimensionerResult ScaleDimensionerResult;

    private Configuration Configuration;

    public Process(Configuration configuration, DatabaseService databaseService, ScaleDimensionerResult scaleDimensionerResult) 
    {
        this.Configuration = configuration;
        this.DatabaseService = databaseService;
        this.ScaleDimensionerResult = scaleDimensionerResult;
    }

    public async Task ProcessPackageAsync(ScaleDimensionerResult scaleDimensionerResult) 
    {
        if (this.ScaleDimensionerResult == null || this.DatabaseService == null) return;

        try 
        {
            string freightLetterNumber = this.ScaleDimensionerResult.Barcode;
            PackageData packageData = await DatabaseService.GetOrderAsync(freightLetterNumber); // 1

            if (packageData == null) return;

            ScanData scanData = await DatabaseService.GetScanAsync(packageData); // 2

            string scanLocation = packageData.df_ndl;
            string date = packageData.df_datauftannahme;
            string orderNumber = packageData.df_lfdnrauftrag;

            if (scanData == null)
            {
                await DatabaseService.AddScanAsync(scaleDimensionerResult, packageData); // 3

                int? realWeight = await DatabaseService.GetWeightAsync(packageData); // 4

                if (realWeight == null) return;

                await DatabaseService.UpdateWeightAsync((int) realWeight, packageData); // 5

                bool? packageExists = await DatabaseService.DoesPackageExistAsync(packageData); // 6

                if (packageExists == null) return;

                float volumeFactor;

                if (packageData.df_ndl == this.Configuration.ScanLocation // 7
                    && this.Configuration.ExceptionList!.ContainsKey(packageData.df_kundennr)) 
                {
                    volumeFactor = this.Configuration.ExceptionList[packageData.df_kundennr];
                } else {
                    volumeFactor = this.Configuration.DefaultVolumeFactor;
                }

                await this.DatabaseService.CreatePackageAsync(packageData, scaleDimensionerResult, volumeFactor); // 8

                int? volumeWeight = await this.DatabaseService.GetTotalWeightAsync(packageData); // ß

                if (volumeWeight == null) return;

                await this.DatabaseService.UpdateTotalWeightAsync((int) volumeWeight, packageData); // 10

                int weight = (int) realWeight;

                if (packageData.df_ndl == this.Configuration.ScanLocation && volumeWeight > realWeight) {
                    weight = (int) volumeWeight;
                } 

                await this.DatabaseService.UpdateOrderWeightAsync((int) realWeight, packageData); // 11
            } 
            else
            {
                await DatabaseService.UpdateScanAsync(scaleDimensionerResult, packageData); // 3

                int? realWeight = await DatabaseService.GetWeightAsync(packageData); // 4

                if (realWeight == null) return;

                await DatabaseService.UpdateWeightAsync((int) realWeight, packageData); // 5

                bool? packageExists = await DatabaseService.DoesPackageExistAsync(packageData); // 6

                if (packageExists == null) return;

                float volumeFactor;

                if (packageData.df_ndl == this.Configuration.ScanLocation // 7
                    && this.Configuration.ExceptionList!.ContainsKey(packageData.df_kundennr)) 
                {
                    volumeFactor = this.Configuration.ExceptionList[packageData.df_kundennr];
                } else {
                    volumeFactor = this.Configuration.DefaultVolumeFactor;
                }

                await this.DatabaseService.UpdatePackageAsync(packageData, scaleDimensionerResult, volumeFactor); // 8

                int? volumeWeight = await this.DatabaseService.GetTotalWeightAsync(packageData); // ß

                if (volumeWeight == null) return;

                await this.DatabaseService.UpdateTotalWeightAsync((int) volumeWeight, packageData); // 10

                int weight = (int) realWeight;

                if (packageData.df_ndl == this.Configuration.ScanLocation && volumeWeight > realWeight) {
                    weight = (int) volumeWeight;
                } 

                await this.DatabaseService.UpdateOrderWeightAsync((int) realWeight, packageData); // 11
            }
        }
        catch (OdbcException e) 
        {
            Console.WriteLine(e.StackTrace);
        }

    }

}