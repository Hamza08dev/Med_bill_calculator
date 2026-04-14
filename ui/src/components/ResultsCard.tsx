import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";

interface ResultsCardProps {
  totalAmount: number;
  region?: string | null;
  providerType?: string | null;
  designation?: string | null;
  calculatedAt: Date | null;
  onClear: () => void;
}

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  minimumFractionDigits: 2,
});

const ResultsCard = ({
  totalAmount,
  region,
  providerType,
  designation,
  calculatedAt,
  onClear,
}: ResultsCardProps) => {
  const timestampLabel = calculatedAt
    ? `Last calculated ${calculatedAt.toLocaleString("en-US", {
        month: "short",
        day: "numeric",
        year: "numeric",
        hour: "numeric",
        minute: "2-digit",
      })}`
    : "Calculated just now";

  return (
    <Card className="shadow-lg border-border/50 bg-muted/30">
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-4">
        <CardTitle className="text-xl">Calculation Summary</CardTitle>
        <Button variant="ghost" size="sm" onClick={onClear} className="text-muted-foreground hover:text-foreground">
          Clear results
        </Button>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-1">
            <p className="text-sm text-muted-foreground">Total Allowed Amount</p>
            <p className="text-3xl font-semibold text-primary">
              {currencyFormatter.format(Number.isFinite(totalAmount) ? totalAmount : 0)}
            </p>
          </div>

          <div className="space-y-1">
            <p className="text-sm text-muted-foreground">Region</p>
            <p className="text-xl font-medium">{region || "Region unavailable"}</p>
          </div>

          <div className="space-y-1">
            <p className="text-sm text-muted-foreground">Provider Type</p>
            <p className="text-sm font-medium text-foreground">
              {providerType ? providerType.charAt(0).toUpperCase() + providerType.slice(1) : "Not provided"}
            </p>
          </div>

          {designation && (
            <div className="space-y-1">
              <p className="text-sm text-muted-foreground">Designation</p>
              <p className="text-sm font-medium">{designation}</p>
            </div>
          )}

          <div className="space-y-1 sm:col-span-2">
            <p className="text-xs text-muted-foreground">{timestampLabel}</p>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};

export default ResultsCard;
