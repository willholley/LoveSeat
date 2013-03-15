using System;
using System.Threading.Tasks;

namespace LoveSeat.Async.Interfaces
{
    public interface IRepository<T> where T : IBaseObject
    {
        Task SaveAsync(T item);
        Task<T> FindAsync(Guid id);
    }
}