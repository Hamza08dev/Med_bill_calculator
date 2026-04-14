import { Link } from "react-router-dom";
import { Calculator } from "lucide-react";

import { Button } from "@/components/ui/button";

const Navbar = () => {
  return (
    <nav className="sticky top-0 z-50 border-b border-white/20 bg-gradient-to-r from-teal-600/90 via-teal-700/80 to-teal-600/90 text-white shadow-md backdrop-blur supports-[backdrop-filter]:bg-white/30">
      <div className="container mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex h-16 items-center justify-between">
          <Link to="/" className="flex items-center gap-2 font-semibold tracking-tight transition-colors hover:text-slate-100">
            <span className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-white/15">
              <Calculator className="h-5 w-5 text-white" />
            </span>
            <span className="text-lg">NY WC Medical Fee Validator</span>
          </Link>

          <div className="flex items-center gap-3 text-sm font-medium uppercase tracking-wide text-slate-100 max-md:text-[0.7rem]">
            <Link to="/" className="rounded-full px-3 py-1 transition-colors hover:bg-white/20 md:bg-white/15 md:px-4">
              Home
            </Link>
            <Link to="/login" className="rounded-full px-3 py-1 transition-colors hover:bg-white/20 md:bg-white/15 md:px-4">
              Login
            </Link>
          </div>

          <Button
            asChild
            size="sm"
            className="bg-white text-teal-700 shadow-lg transition hover:bg-slate-50"
          >
            <Link to="/signup">Get Started</Link>
          </Button>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;
