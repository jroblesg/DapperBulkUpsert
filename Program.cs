using System;
using System.Threading.Tasks;

namespace BulkUpsertDapper
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("SELECCIONE UNA OPCIÓN:");
            Console.WriteLine("1. BULK-INSERT");
            Console.WriteLine("2. BULK-UPDATE");
            string option = Console.ReadLine();
            switch (option)
            {
                case "1":
                    await BulkInsert();
                    break;
                case "2":
                    await BulkUpdate();
                    break;
                default:
                    await BulkInsert();
                    break;
            }

            Console.WriteLine("Good bye, press any key to coronavirus***");
            Console.ReadLine();
        }

        private static async Task BulkUpdate()
        {
            Console.Write("Inserte nuevo nombre (se actualizaran registros entre el 100 y el 1500: ");
            var newName = Console.ReadLine();
            Console.WriteLine("Wait...");
            var service = new BulkUpsertService();
            int res = await service.GoBulkUpdate(newName);
            Console.WriteLine($"Se actualizaron {res} registros");
        }

        private static async Task BulkInsert()
        {
            Console.Write("Cuantos registros quiere insertar?: ");
            var registersToInsert = Console.ReadLine();
            Console.WriteLine("Wait...");
            var service = new BulkUpsertService();
            int res = await service.GoBulkInsert(int.Parse(registersToInsert));
            Console.WriteLine($"Se insertaron {res} registros");
        }
        
    }
}
