import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { X } from "lucide-react";

interface CptRowProps {
  id: string;
  cptCode: string;
  units: string;
  billedAmount: string;
  onUpdate: (id: string, field: "cptCode" | "units" | "billedAmount", value: string) => void;
  onRemove: (id: string) => void;
  canRemove: boolean;
  error?: string;
}

const CptRow = ({
  id,
  cptCode,
  units,
  billedAmount,
  onUpdate,
  onRemove,
  canRemove,
  error,
}: CptRowProps) => {
  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <div className="flex-1">
          <Input
            placeholder="CPT Code"
            value={cptCode}
            onChange={(e) => onUpdate(id, "cptCode", e.target.value)}
            className={`font-mono ${error ? "border-destructive" : ""}`}
            aria-label="CPT Code"
          />
        </div>
        <div className="w-20">
          <Input
            type="number"
            placeholder="1"
            min="1"
            value={units}
            onChange={(e) => onUpdate(id, "units", e.target.value)}
            className="text-center"
            aria-label="Units"
          />
        </div>
        {canRemove && (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={() => onRemove(id)}
            className="h-10 w-10 text-slate-400 hover:text-destructive"
            aria-label="Remove CPT row"
          >
            <X className="h-4 w-4" />
          </Button>
        )}
      </div>
      {error && <p className="text-xs text-destructive ml-1">{error}</p>}
    </div>
  );
};

export default CptRow;
