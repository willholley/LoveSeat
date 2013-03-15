using System;
using System.Threading.Tasks;
using LoveSeat.Async.Interfaces;

namespace LoveSeat.Async.Repositories
{
    public abstract class AuditableRepository<T> : CouchRepository<T> where T : IAuditableRecord
    {
        protected AuditableRepository(CouchDatabase db) : base(db)
        {
        }
        public async override Task SaveAsync(T item)
        {
            if (item.Rev == null)
                item.CreatedAt = DateTime.Now;
            item.LastModifiedAt = DateTime.Now;
            if (item.Id == string.Empty)
                item.Id = Guid.NewGuid().ToString();    
            await base.SaveAsync(item);
        }
    }
}