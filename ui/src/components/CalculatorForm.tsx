import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { Loader2, Plus, ChevronUp, ChevronDown } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import { toast } from "@/components/ui/use-toast";

import CptRow from "./CptRow";
import ResultsTable from "./ResultsTable";
import ComingSoonCard from "./ComingSoonCard";

type ProviderType = "medical" | "podiatry" | "chiropractic";

interface CptRowData {
  id: string;
  cptCode: string;
  units: string;
  billedAmount: string;
}

interface CalculatorLineResult {
  cpt_code?: string | null;
  calculated_fee?: number | null;
  modifier_applied?: string | null;
  rvu?: number | null;
  conversion_factor?: number | null;
  schedule?: string | null;
  units?: number | null;
  explanation?: string | null;
  global_fee?: number | null;
}

interface CalculatorResponse {
  total_calculated_amount: number;
  region?: string | null;
  provider_type?: string | null;
  designation?: string | null;
  line_results: CalculatorLineResult[];
}

interface CalculatorRequestPayload {
  zip_code: string;
  provider_type: ProviderType;
  designation?: string;
  is_np_pa: boolean;
  skip_ground_rules?: boolean;
  lines: Array<{
    code: string;
    units: number;
    billed_amount?: number;
  }>;
}

const providerOptions: Array<{ label: string; value: ProviderType }> = [
  { label: "Medical / Behavioral", value: "medical" },
  { label: "Chiropractic", value: "chiropractic" },
];

const createEmptyRow = (id: number): CptRowData => ({
  id: id.toString(),
  cptCode: "",
  units: "1",
  billedAmount: "",
});

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

const CalculatorForm = () => {
  const [serviceZip, setServiceZip] = useState("12345");
  const [providerType, setProviderType] = useState<ProviderType>("medical");
  const [isNpPa, setIsNpPa] = useState(true);
  const [cptRows, setCptRows] = useState<CptRowData[]>([
    { id: "1", cptCode: "123455", units: "1", billedAmount: "" },
  ]);
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [baseFeeResults, setBaseFeeResults] = useState<CalculatorResponse | null>(null);
  const [modifiersResults, setModifiersResults] = useState<CalculatorResponse | null>(null);
  const [calculatedAt, setCalculatedAt] = useState<Date | null>(null);
  const [showDetails, setShowDetails] = useState(false);
  const [activeTab, setActiveTab] = useState<"base" | "modifiers">("base");

  const baseFeeMutation = useMutation({
    mutationFn: async (payload: CalculatorRequestPayload): Promise<CalculatorResponse> => {
      const baseUrl = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "");
      if (!baseUrl) {
        throw new Error("VITE_API_BASE_URL is not configured yet.");
      }

      const response = await fetch(`${baseUrl}/v1/fees/calculate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...payload, skip_ground_rules: true }),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data?.detail || "Unable to calculate fees right now.");
      }

      return data as CalculatorResponse;
    },
    onSuccess: (data) => {
      setBaseFeeResults(data);
      setCalculatedAt(new Date());
    },
    onError: (error: Error) => {
      toast({
        title: "Calculation failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const modifiersMutation = useMutation({
    mutationFn: async (payload: CalculatorRequestPayload): Promise<CalculatorResponse> => {
      const baseUrl = import.meta.env.VITE_API_BASE_URL?.replace(/\/$/, "");
      if (!baseUrl) {
        throw new Error("VITE_API_BASE_URL is not configured yet.");
      }

      const response = await fetch(`${baseUrl}/v1/fees/calculate`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ...payload, skip_ground_rules: false }),
      });

      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(data?.detail || "Unable to calculate fees right now.");
      }

      return data as CalculatorResponse;
    },
    onSuccess: (data) => {
      setModifiersResults(data);
      setCalculatedAt(new Date());
    },
    onError: (error: Error) => {
      toast({
        title: "Calculation failed",
        description: error.message,
        variant: "destructive",
      });
    },
  });

  const addCptRow = () => {
    const nextId = Math.max(...cptRows.map((row) => Number(row.id))) + 1;
    setCptRows((prev) => [...prev, createEmptyRow(nextId)]);
  };

  const removeCptRow = (id: string) => {
    if (cptRows.length === 1) return;
    setCptRows((prev) => prev.filter((row) => row.id !== id));
    setErrors((prev) => {
      const next = { ...prev };
      delete next[`cpt-${id}`];
      return next;
    });
  };

  const updateCptRow = (id: string, field: "cptCode" | "units" | "billedAmount", value: string) => {
    setCptRows((prev) =>
      prev.map((row) =>
        row.id === id
          ? {
              ...row,
              [field]: value,
            }
          : row,
      ),
    );

    if (field === "cptCode" && errors[`cpt-${id}`]) {
      setErrors((prev) => {
        const next = { ...prev };
        delete next[`cpt-${id}`];
        return next;
      });
    }
  };

  const validateForm = () => {
    const nextErrors: Record<string, string> = {};
    const zip = serviceZip.trim();

    if (!zip) {
      nextErrors.serviceZip = "Service ZIP code is required.";
    } else if (!/^\d{5}$/.test(zip)) {
      nextErrors.serviceZip = "ZIP code must be exactly 5 digits.";
    }

    const rowsWithCodes = cptRows.filter((row) => row.cptCode.trim());
    if (!rowsWithCodes.length) {
      nextErrors.lines = "Enter at least one CPT code.";
    }

    cptRows.forEach((row) => {
      if (!row.cptCode.trim()) {
        nextErrors[`cpt-${row.id}`] = "CPT code is required.";
      }
    });

    setErrors(nextErrors);
    return {
      isValid: Object.keys(nextErrors).length === 0,
      sanitizedRows: rowsWithCodes,
    };
  };

  const handleCalculate = () => {
    const { isValid, sanitizedRows } = validateForm();
    if (!isValid) return;

    const effectiveDesignation = isNpPa ? "NP/PA" : undefined;

    const payload: CalculatorRequestPayload = {
      zip_code: serviceZip.trim(),
      provider_type: providerType,
      designation: effectiveDesignation,
      is_np_pa: isNpPa,
      lines: sanitizedRows.map((row) => ({
        code: row.cptCode.trim(),
        units: Math.max(parseInt(row.units, 10) || 1, 1),
        billed_amount: row.billedAmount ? Number(row.billedAmount) : undefined,
      })),
    };

    baseFeeMutation.mutate(payload);
    modifiersMutation.mutate(payload);
  };

  const clearResults = () => {
    setBaseFeeResults(null);
    setModifiersResults(null);
    setCalculatedAt(null);
    setShowDetails(false);
  };

  const currentResults = activeTab === "base" ? baseFeeResults : modifiersResults;
  const isLoading = baseFeeMutation.isPending || modifiersMutation.isPending;

  return (
    <div className="mx-auto w-full max-w-7xl space-y-6 px-2 sm:px-4 lg:px-0">
      <div className="grid gap-6 lg:grid-cols-2">
        {/* Left Column: Calculator */}
        <Card className="border-white/60 bg-white/80 shadow-xl ring-1 ring-teal-100/60 backdrop-blur">
          <CardContent className="space-y-6 p-6">
            <div>
              <h2 className="text-xl font-semibold text-slate-900 mb-2">Calculator</h2>
              <p className="text-sm text-slate-600">Enter your service details below.</p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="service-zip" className="text-sm font-medium">
                Service ZIP Code <span className="text-destructive">*</span>
              </Label>
              <Input
                id="service-zip"
                placeholder="12345"
                value={serviceZip}
                onChange={(event) => {
                  setServiceZip(event.target.value);
                  if (errors.serviceZip) {
                    setErrors((prev) => {
                      const next = { ...prev };
                      delete next.serviceZip;
                      return next;
                    });
                  }
                }}
                maxLength={5}
                className={errors.serviceZip ? "border-destructive" : ""}
                aria-invalid={!!errors.serviceZip}
              />
              {errors.serviceZip && (
                <p className="text-xs text-destructive">{errors.serviceZip}</p>
              )}
            </div>

            <div className="space-y-2">
              <Label className="text-sm font-medium">Provider Type</Label>
              <div className="flex gap-2">
                {providerOptions.map((option) => (
                  <Button
                    key={option.value}
                    type="button"
                    variant={providerType === option.value ? "default" : "outline"}
                    className={`flex-1 text-sm ${
                      providerType === option.value
                        ? "bg-teal-600 hover:bg-teal-700 text-white border-teal-600"
                        : "bg-white hover:bg-teal-50 text-slate-700 border-slate-300"
                    }`}
                    onClick={() => setProviderType(option.value)}
                  >
                    {option.label}
                  </Button>
                ))}
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-start space-x-2 rounded-md border border-slate-200 bg-slate-50/50 p-3">
                <Checkbox
                  id="np-pa"
                  checked={isNpPa}
                  onCheckedChange={(checked) => setIsNpPa(!!checked)}
                  className="mt-0.5"
                />
                <Label
                  htmlFor="np-pa"
                  className="text-sm font-normal leading-snug cursor-pointer"
                >
                  NP/PA (automatically applies 80% modifier, otherwise assumes MD/DO)
                </Label>
              </div>
            </div>

            <div className="space-y-4">
              <Label className="text-sm font-medium">
                CPT Codes <span className="text-destructive">*</span>
              </Label>
              <div className="space-y-3">
                {cptRows.map((row) => (
                  <CptRow
                    key={row.id}
                    id={row.id}
                    cptCode={row.cptCode}
                    units={row.units}
                    billedAmount={row.billedAmount}
                    onUpdate={updateCptRow}
                    onRemove={removeCptRow}
                    canRemove={cptRows.length > 1}
                    error={errors[`cpt-${row.id}`]}
                  />
                ))}
              </div>
              {errors.lines && <p className="text-xs text-destructive">{errors.lines}</p>}
              <button
                type="button"
                onClick={addCptRow}
                className="w-full text-sm text-teal-600 hover:text-teal-700 font-medium text-left"
              >
                + Add CPT
              </button>
            </div>

            <Button
              type="button"
              onClick={handleCalculate}
              className="w-full bg-teal-600 hover:bg-teal-700 text-white"
              size="lg"
              disabled={isLoading}
            >
              {isLoading ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Calculating…
                </>
              ) : (
                "Calculate Fee"
              )}
            </Button>
          </CardContent>
        </Card>

        {/* Right Column: Results */}
        <div className="flex flex-col h-fit">
          {baseFeeResults || modifiersResults ? (
            <>
              {/* Top: Total Fee Card with White Background (40%) */}
              <Card className="bg-white border-white/60 shadow-xl ring-1 ring-teal-100/60 backdrop-blur rounded-b-none border-b-0">
                <CardContent className="pt-6 pb-6 min-h-[40%]">
                  <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "base" | "modifiers")} className="w-full">
                    <div className="flex justify-center mb-6">
                      <TabsList className="grid grid-cols-2 bg-slate-100 h-9 w-auto">
                        <TabsTrigger
                          value="base"
                          className="data-[state=active]:bg-teal-600 data-[state=active]:text-white text-xs px-4"
                        >
                          Base Fee
                        </TabsTrigger>
                        <TabsTrigger
                          value="modifiers"
                          className="data-[state=active]:bg-teal-600 data-[state=active]:text-white text-xs px-4"
                        >
                          With Modifiers (ground rules included)
                        </TabsTrigger>
                      </TabsList>
                    </div>

                    <TabsContent value="base" className="mt-0">
                      {baseFeeResults ? (
                        <div className="text-center">
                          <p className="text-5xl font-bold text-slate-800 mb-2">
                            {currencyFormatter.format(baseFeeResults.total_calculated_amount || 0)}
                          </p>
                          <p className="text-sm text-slate-500">NYS Allowed Fee (2024)</p>
                        </div>
                      ) : (
                        <div className="text-center py-8">
                          <p className="text-slate-500">Calculating base fee...</p>
                        </div>
                      )}
                    </TabsContent>

                    <TabsContent value="modifiers" className="mt-0">
                      {modifiersResults ? (
                        <div className="text-center">
                          <p className="text-5xl font-bold text-slate-800 mb-2">
                            {currencyFormatter.format(modifiersResults.total_calculated_amount || 0)}
                          </p>
                          <p className="text-sm text-slate-500">NYS Allowed Fee (2024)</p>
                        </div>
                      ) : (
                        <div className="text-center py-8">
                          <p className="text-slate-500">Calculating with modifiers...</p>
                        </div>
                      )}
                    </TabsContent>
                  </Tabs>
                </CardContent>
              </Card>

              {/* Bottom: Details Card with Teal Background (60%) */}
              <Card className="bg-teal-600 text-white shadow-xl border-teal-600 rounded-t-none">
                <CardContent className="pt-6 pb-6 space-y-4 min-h-[60%]">
                  <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as "base" | "modifiers")} className="w-full">
                    <TabsContent value="base" className="mt-0">
                      {baseFeeResults ? (
                        <>
                          <div className="grid grid-cols-2 gap-4 text-sm">
                            <div>
                              <p className="text-teal-200 mb-1">CPT Codes</p>
                              <p className="text-white font-medium">
                                {baseFeeResults.line_results
                                  .map((r) => r.cpt_code)
                                  .filter(Boolean)
                                  .join(", ") || "—"}
                              </p>
                            </div>
                            <div>
                              <p className="text-teal-200 mb-1">Region</p>
                              <p className="text-white font-medium">
                                {baseFeeResults.region
                                  ? `${baseFeeResults.region} (ZIP: ${serviceZip})`
                                  : "—"}
                              </p>
                            </div>
                            <div>
                              <p className="text-teal-200 mb-1">Provider Type</p>
                              <p className="text-white font-medium">
                                {providerOptions.find((p) => p.value === providerType)?.label || "—"}
                              </p>
                            </div>
                            <div>
                              <p className="text-teal-200 mb-1">Designation</p>
                              <p className="text-white font-medium">{isNpPa ? "NP/PA" : "MD/DO"}</p>
                            </div>
                          </div>

                          <div className="pt-4 border-t border-teal-500/30">
                            <button
                              onClick={() => setShowDetails(!showDetails)}
                              className="w-full flex items-center justify-center gap-2 text-sm text-white bg-teal-700 hover:bg-teal-800 px-4 py-2 rounded-md font-medium transition-colors"
                            >
                              Detailed Breakdown
                              {showDetails ? (
                                <ChevronUp className="h-4 w-4" />
                              ) : (
                                <ChevronDown className="h-4 w-4" />
                              )}
                            </button>
                          </div>

                          {calculatedAt && (
                            <p className="text-xs text-teal-200 text-center pt-2">
                              Last calculated {calculatedAt.toLocaleString("en-US", {
                                month: "short",
                                day: "numeric",
                                year: "numeric",
                                hour: "numeric",
                                minute: "2-digit",
                              })}
                            </p>
                          )}
                        </>
                      ) : null}
                    </TabsContent>

                    <TabsContent value="modifiers" className="mt-0">
                      {modifiersResults ? (
                        <>
                          <div className="grid grid-cols-2 gap-4 text-sm">
                            <div>
                              <p className="text-teal-200 mb-1">CPT Codes</p>
                              <p className="text-white font-medium">
                                {modifiersResults.line_results
                                  .map((r) => r.cpt_code)
                                  .filter(Boolean)
                                  .join(", ") || "—"}
                              </p>
                            </div>
                            <div>
                              <p className="text-teal-200 mb-1">Region</p>
                              <p className="text-white font-medium">
                                {modifiersResults.region
                                  ? `${modifiersResults.region} (ZIP: ${serviceZip})`
                                  : "—"}
                              </p>
                            </div>
                            <div>
                              <p className="text-teal-200 mb-1">Provider Type</p>
                              <p className="text-white font-medium">
                                {providerOptions.find((p) => p.value === providerType)?.label || "—"}
                              </p>
                            </div>
                            <div>
                              <p className="text-teal-200 mb-1">Designation</p>
                              <p className="text-white font-medium">{isNpPa ? "NP/PA" : "MD/DO"}</p>
                            </div>
                          </div>

                          <div className="pt-4 border-t border-teal-500/30">
                            <button
                              onClick={() => setShowDetails(!showDetails)}
                              className="w-full flex items-center justify-center gap-2 text-sm text-white bg-teal-700 hover:bg-teal-800 px-4 py-2 rounded-md font-medium transition-colors"
                            >
                              Detailed Breakdown
                              {showDetails ? (
                                <ChevronUp className="h-4 w-4" />
                              ) : (
                                <ChevronDown className="h-4 w-4" />
                              )}
                            </button>
                          </div>

                          {calculatedAt && (
                            <p className="text-xs text-teal-200 text-center pt-2">
                              Last calculated {calculatedAt.toLocaleString("en-US", {
                                month: "short",
                                day: "numeric",
                                year: "numeric",
                                hour: "numeric",
                                minute: "2-digit",
                              })}
                            </p>
                          )}
                        </>
                      ) : null}
                    </TabsContent>
                  </Tabs>
                </CardContent>
              </Card>
            </>
          ) : (
            <Card className="bg-white/80 border-white/60 shadow-xl ring-1 ring-teal-100/60 backdrop-blur">
              <CardContent className="pt-6 pb-6 flex items-center justify-center min-h-[400px]">
                <p className="text-slate-500">Calculate fees to see results here</p>
              </CardContent>
            </Card>
          )}
        </div>
      </div>

      {/* Detailed Breakdown Section */}
      {showDetails && (baseFeeResults || modifiersResults) && (
        <div className="mt-6">
          <ResultsTable results={(activeTab === "base" ? baseFeeResults : modifiersResults)?.line_results || []} />
        </div>
      )}

      <ComingSoonCard />
    </div>
  );
};

export default CalculatorForm;
