import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Upload, FileText } from "lucide-react";

const ComingSoonCard = () => {
  return (
    <Card className="shadow-lg border-border/50 bg-gradient-to-br from-muted/20 to-muted/40">
      <CardHeader>
        <CardTitle className="text-xl">Coming Soon: Automated Bill Extraction</CardTitle>
        <CardDescription>
          Upload medical bills or enter case numbers to automatically extract and calculate CPT fees
        </CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Button
              variant="outline"
              className="w-full"
              disabled
              aria-disabled="true"
            >
              <Upload className="h-4 w-4 mr-2" />
              Upload NF-3 / CMS-1500
            </Button>
            <p className="text-xs text-muted-foreground">
              Automatically extract CPT codes, modifiers, and billing details from standard medical forms
            </p>
          </div>
          
          <div className="space-y-2">
            <Button
              variant="outline"
              className="w-full"
              disabled
              aria-disabled="true"
            >
              <FileText className="h-4 w-4 mr-2" />
              Process Case Number
            </Button>
            <p className="text-xs text-muted-foreground">
              Enter a Workers' Comp case number to retrieve and calculate all associated medical bills
            </p>
          </div>
        </div>
        
        <div className="pt-2 border-t border-border/50">
          <p className="text-xs text-muted-foreground">
            These features will streamline your workflow by eliminating manual data entry. 
            Sign up for early access notifications.
          </p>
        </div>
      </CardContent>
    </Card>
  );
};

export default ComingSoonCard;
