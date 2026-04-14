import Navbar from "@/components/Navbar";
import Footer from "@/components/Footer";
import CalculatorForm from "@/components/CalculatorForm";

const Index = () => {
  return (
    <div className="flex min-h-screen flex-col bg-gradient-to-b from-teal-50/60 via-emerald-50/40 to-white">
      <Navbar />

      <main className="relative flex-1 overflow-hidden">
        <div className="pointer-events-none absolute inset-0">
          <div className="absolute -left-24 top-10 h-72 w-72 rounded-full bg-teal-400/20 blur-3xl" />
          <div className="absolute right-0 top-40 h-80 w-80 rounded-full bg-emerald-300/20 blur-3xl" />
        </div>

        <section className="relative py-12 sm:py-16 lg:py-20">
          <div className="container mx-auto px-4 sm:px-6 lg:px-10">
            <div className="mx-auto mb-12 max-w-3xl text-center">
              <span className="mb-4 inline-flex items-center rounded-full bg-white/70 px-4 py-1 text-xs font-semibold uppercase tracking-wide text-teal-700 shadow-sm">
                Built for New York providers
              </span>
              <h1 className="mb-6 text-4xl font-bold tracking-tight text-slate-900 sm:text-5xl lg:text-6xl">
                New York Instant Medical Fee Calculation
              </h1>
              <p className="mx-auto max-w-2xl text-lg text-slate-600 sm:text-xl">
                Know the New York-approved price for any CPT code, in seconds.
              </p>
            </div>

            <CalculatorForm />
          </div>
        </section>
      </main>

      <Footer />
    </div>
  );
};

export default Index;
