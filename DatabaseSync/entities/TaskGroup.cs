using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DatabaseSync.Entities
{
    public class TaskGroup
    {
        [Key]
        public int Taskgroup_Id { get; set; }
        public string Name { get; set; }
        public Guid? Guid { get; set; }
        public string? GlobalID { get; set; } 

    }
}