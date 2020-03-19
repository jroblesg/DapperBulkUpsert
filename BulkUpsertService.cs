using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BulkUpsertDapper
{
    public class BulkUpsertService
    {
        private readonly IConfigurationRoot _config;
        private readonly string connString = "Server=localhost;Database=bulkdb;Trusted_Connection=True;";

        private Random random = new Random();

        public string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        public async Task<int> GoBulkInsert(int? number = 100)
        {
            var people = new List<PersonInsert>();
            for (int i = 0; i < number; i++)
            {
                people.Add(new PersonInsert { Name = RandomString(10) });
            }
            var _repository = new GenericRepository<PersonInsert>("Person", connString);
            var inserted = await _repository.BulkInsertAsync(people);
            return inserted;
        }

        public async Task<int> GoBulkUpdate(string NewName)
        {
            var _repository = new GenericRepository<Person>("Person", connString);
            var people = await _repository.GetAllAsync();
            IList<Person> toEdit = people.ToList().Skip(100).Take(1500).ToList();
            foreach (var i in toEdit)
                i.Name = NewName;
            var updated= await _repository.BulkUpdateAsync(toEdit);
            return updated;
        }
    }

    
    

    public class Person
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class PersonInsert
    {
        public string Name { get; set; }
    }

   
}
