import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Info } from "lucide-react";

interface CalculatorLineResult {
  cpt_code?: string | null;
  calculated_fee?: number | null;
  modifier_applied?: string | null;
  rvu?: number | null;
  conversion_factor?: number | null;
  schedule?: string | null;
  units?: number | null;
  explanation?: string | null;
}

interface ResultsTableProps {
  results: CalculatorLineResult[];
}

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

const toNumber = (value: number | string | null | undefined): number | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim() !== "" && !Number.isNaN(Number(value))) {
    return Number(value);
  }
  return null;
};

const formatConversion = (value: number | string | null | undefined) => {
  const numeric = toNumber(value);
  return numeric == null ? "—" : numeric.toFixed(2);
};

// For now, show modifiers exactly as provided by the backend.
// Example: "PA/NP (80%), SGR5 (50%)" or "PA/NP (80%), RGR3 (75%)"
const formatModifiers = (modifierString: string | null | undefined): string => {
  if (!modifierString) return "—";
  const trimmed = modifierString.trim();
  return trimmed.length ? trimmed : "—";
};

const ResultsTable = ({ results }: ResultsTableProps) => {
  if (!results.length) {
    return null;
  }

  return (
    <Card className="shadow-lg border-border/50">
      <CardHeader>
        <CardTitle className="text-xl flex items-center gap-2">
          Detailed Breakdown
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger>
                <Info className="h-4 w-4 text-muted-foreground" />
              </TooltipTrigger>
              <TooltipContent>
                <p className="max-w-xs">
                  Fees calculated using the New York Workers’ Compensation fee schedule with the RVUs and conversion factors stored in the knowledge graph.
                </p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="hidden md:block overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>CPT Code</TableHead>
                <TableHead className="text-right">Units</TableHead>
                <TableHead className="text-right">Allowed Fee</TableHead>
                <TableHead className="text-right">Total</TableHead>
                <TableHead>Modifiers</TableHead>
                <TableHead className="text-right">RVU</TableHead>
                <TableHead className="text-right">Conv. Factor</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {results.map((line, index) => {
                const perUnit = toNumber(line.calculated_fee) ?? 0;
                const units = toNumber(line.units) ?? 1;
                const total = perUnit * units;
                return (
                  <TableRow key={`${line.cpt_code}-${index}`}>
                    <TableCell className="font-mono font-medium">{line.cpt_code || "—"}</TableCell>
                    <TableCell className="text-right">{units}</TableCell>
                    <TableCell className="text-right font-medium">{currencyFormatter.format(perUnit)}</TableCell>
                    <TableCell className="text-right font-semibold">{currencyFormatter.format(total)}</TableCell>
                    <TableCell className="text-sm text-muted-foreground">
                      {formatModifiers(line.modifier_applied)}
                    </TableCell>
                    <TableCell className="text-right text-sm">{line.rvu ? formatConversion(line.rvu) : "—"}</TableCell>
                    <TableCell className="text-right text-sm">{formatConversion(line.conversion_factor)}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>

        <div className="md:hidden">
          <Accordion type="single" collapsible className="w-full">
            {results.map((line, index) => {
              const perUnit = toNumber(line.calculated_fee) ?? 0;
              const units = toNumber(line.units) ?? 1;
              const total = perUnit * units;
              return (
                <AccordionItem key={`${line.cpt_code}-${index}`} value={`item-${index}`}>
                  <AccordionTrigger className="hover:no-underline">
                    <div className="flex w-full items-center justify-between pr-4">
                      <span className="font-mono font-medium">{line.cpt_code || "—"}</span>
                      <span className="font-semibold">{currencyFormatter.format(total)}</span>
                    </div>
                  </AccordionTrigger>
                  <AccordionContent>
                    <div className="space-y-3 pt-2">
                      <div className="grid grid-cols-2 gap-3 text-sm">
                        <div>
                          <p className="text-xs text-muted-foreground">Units</p>
                          <p className="font-medium">{units}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Allowed / Unit</p>
                          <p className="font-medium">{currencyFormatter.format(perUnit)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Modifiers</p>
                          <p>{formatModifiers(line.modifier_applied)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">RVU</p>
                          <p>{line.rvu ?? "—"}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Conversion Factor</p>
                          <p>{formatConversion(line.conversion_factor)}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Schedule</p>
                          <p>{line.schedule || "—"}</p>
                        </div>
                      </div>
                      {line.explanation && (
                        <div>
                          <p className="text-xs text-muted-foreground">Explanation</p>
                          <p className="text-sm">{line.explanation}</p>
                        </div>
                      )}
                    </div>
                  </AccordionContent>
                </AccordionItem>
              );
            })}
          </Accordion>
        </div>

        <div className="mt-4 rounded-md bg-slate-50 p-3">
          <p className="text-xs text-slate-600">
            <strong>Note:</strong> All calculations based on New York Workers' Compensation Medical Fee Schedule. Modifiers automatically applied per state regulations.
          </p>
        </div>
      </CardContent>
    </Card>
  );
};

export default ResultsTable;
