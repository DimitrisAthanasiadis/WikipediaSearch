using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using WikipediaSearchService;

namespace WikipediaSearch.Controllers
{
    public class HomeController : Controller
    {
        private static LuceneIndexService SearchService = new LuceneIndexService(@"C:\temp", false);

        public ActionResult Index()
        {
            ViewBag.TotalDocs = SearchService.Count();
            return View();
        }

        public JsonResult Search(string data)
        {
            if (string.IsNullOrEmpty(data)) return Json(new string[] { }, JsonRequestBehavior.AllowGet);

            try
            {
                var results = SearchService.Search(data);
                var r = results.Select(item =>
                    new
                    {
                        Score = item.Item1,
                        Title = item.Item2.GetValues("title")[0],
                        Id = item.Item2.GetValues("id")[0]
                    }
                ).ToList();
                return Json(r, JsonRequestBehavior.AllowGet);
            }
            catch (Exception e) 
            {
                this.Response.StatusCode = 500;
                return Json(new string[]{}, JsonRequestBehavior.AllowGet);
            }
        }
    }
}