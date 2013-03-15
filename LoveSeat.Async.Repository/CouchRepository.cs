using System;
using System.Threading.Tasks;
using LoveSeat.Async.Interfaces;

namespace LoveSeat.Async.Repositories
{
    public class CouchRepository<T> : IRepository<T> where T : IBaseObject
    {
        protected readonly CouchDatabase db = null;
        public  CouchRepository(CouchDatabase db)
        {
            this.db = db;
        }

        public async virtual Task SaveAsync(T item)
        {
            if (item.Id == "")
                item.Id = Guid.NewGuid().ToString();
            var doc = new Document<T>(item);
            await db.SaveDocumentAsync(doc);
        }

        public async virtual Task<T> FindAsync(Guid id)
        {
            return await db.GetDocumentAsync<T>(id.ToString());
        }

        /// <summary>
        /// Repository methods don't have the business validation.  Use the service methods to enforce.
        /// </summary>
        /// <param name="obj"></param>
        public async virtual Task Delete(T obj)
        {
            await db.DeleteDocumentAsync(obj.Id, obj.Rev);
        }
    }
}